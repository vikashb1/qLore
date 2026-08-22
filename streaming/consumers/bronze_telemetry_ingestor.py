import json
from datetime import datetime

import pyarrow as pa

from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog import load_catalog


# ============================================================
# Kafka Configuration
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC = "qlore.telemetry.v1"

CONSUMER_GROUP = "qlore-bronze-telemetry-ingestion"

BATCH_SIZE = 10


# ============================================================
# Iceberg Configuration
# ============================================================

CATALOG_URI = "http://localhost:8181"

WAREHOUSE = "s3://warehouse"


def get_catalog():

    return load_catalog(
        "qlore",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


# ============================================================
# Convert Kafka record → Bronze record
# ============================================================

def build_bronze_record(message):

    value = json.loads(
        message.value().decode("utf-8")
    )

    event_timestamp = datetime.fromisoformat(
        value["event_timestamp"]
    )

    # Iceberg TimestampType does not contain timezone information.
    # Convert UTC-aware datetime to naive UTC.
    if event_timestamp.tzinfo is not None:

        event_timestamp = (
            event_timestamp
            .astimezone()
            .replace(tzinfo=None)
        )

    record = {

        # ----------------------------
        # Source telemetry
        # ----------------------------

        "device_id":
            value["device_id"],

        "event_timestamp":
            event_timestamp,

        "temperature_mk":
            value.get("temperature_mk"),

        "cpu_usage_pct":
            value.get("cpu_usage_pct"),

        "memory_usage_pct":
            value.get("memory_usage_pct"),

        "queue_depth":
            value.get("queue_depth"),

        "active_jobs":
            value.get("active_jobs"),

        "signal_noise_ratio":
            value.get("signal_noise_ratio"),

        # These fields don't exist in V1.
        # value.get() therefore returns None.
        "cryogenic_pressure_mbar":
            value.get(
                "cryogenic_pressure_mbar"
            ),

        "hardware_error_rate":
            value.get(
                "hardware_error_rate"
            ),

        "system_status":
            value.get("system_status"),

        "schema_version":
            value.get(
                "schema_version",
                "v1",
            ),

        # ----------------------------
        # qLore ingestion metadata
        # ----------------------------

        "ingested_at":
            datetime.utcnow(),

        "kafka_partition":
            message.partition(),

        "kafka_offset":
            message.offset(),
    }

    return record


# ============================================================
# Convert Python records → Arrow
# ============================================================

def records_to_arrow(records):

    schema = pa.schema(
        [
            pa.field(
                "device_id",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "event_timestamp",
                pa.timestamp("us"),
                nullable=False,
            ),

            pa.field(
                "temperature_mk",
                pa.float64(),
            ),

            pa.field(
                "cpu_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "memory_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "queue_depth",
                pa.int32(),
            ),

            pa.field(
                "active_jobs",
                pa.int32(),
            ),

            pa.field(
                "signal_noise_ratio",
                pa.float64(),
            ),

            pa.field(
                "cryogenic_pressure_mbar",
                pa.float64(),
            ),

            pa.field(
                "hardware_error_rate",
                pa.float64(),
            ),

            pa.field(
                "system_status",
                pa.string(),
            ),

            pa.field(
                "schema_version",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "ingested_at",
                pa.timestamp("us"),
            ),

            pa.field(
                "kafka_partition",
                pa.int32(),
            ),

            pa.field(
                "kafka_offset",
                pa.int64(),
            ),
        ]
    )

    return pa.Table.from_pylist(
        records,
        schema=schema,
    )


# ============================================================
# Write micro-batch → Iceberg
# ============================================================

def write_batch(
    table,
    records,
):

    if not records:
        return

    arrow_table = records_to_arrow(
        records
    )

    table.append(
        arrow_table
    )

    print(
        f"\nWrote {len(records)} "
        f"records to bronze.telemetry"
    )


# ============================================================
# Main ingestion loop
# ============================================================

def main():

    print(
        "\nqLore Kafka → Bronze Telemetry Ingestor"
    )

    print("=" * 70)

    # ----------------------------
    # Connect to Iceberg
    # ----------------------------

    print(
        "\nConnecting to Iceberg..."
    )

    catalog = get_catalog()

    table = catalog.load_table(
        ("bronze", "telemetry")
    )

    print(
        "Loaded bronze.telemetry"
    )

    # ----------------------------
    # Connect to Kafka
    # ----------------------------

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            "auto.offset.reset":
                "earliest",

            # Important:
            # commit only after successful
            # Iceberg write.
            "enable.auto.commit":
                False,
        }
    )

    consumer.subscribe(
        [TOPIC]
    )

    print(
        f"Subscribed to {TOPIC}"
    )

    print(
        f"Consumer group: "
        f"{CONSUMER_GROUP}"
    )

    print(
        f"Micro-batch size: "
        f"{BATCH_SIZE}"
    )

    print(
        "\nWaiting for telemetry..."
    )

    batch = []

    try:

        while True:

            message = consumer.poll(
                timeout=1.0
            )

            if message is None:
                continue

            if message.error():

                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                print(
                    f"Kafka error: "
                    f"{message.error()}"
                )

                continue

            record = build_bronze_record(
                message
            )

            batch.append(
                record
            )

            print(
                f"Buffered "
                f"device={record['device_id']} "
                f"partition={record['kafka_partition']} "
                f"offset={record['kafka_offset']} "
                f"schema={record['schema_version']} "
                f"batch={len(batch)}/{BATCH_SIZE}"
            )

            if len(batch) >= BATCH_SIZE:

                write_batch(
                    table,
                    batch,
                )

                # Commit Kafka offsets only
                # after Iceberg append succeeds.
                consumer.commit(
                    asynchronous=False
                )

                print(
                    "Kafka offsets committed."
                )

                batch.clear()

    except KeyboardInterrupt:

        print(
            "\nStopping Bronze ingestor..."
        )

        # Flush remaining records before exit.
        if batch:

            print(
                f"Writing final "
                f"{len(batch)} records..."
            )

            write_batch(
                table,
                batch,
            )

            consumer.commit(
                asynchronous=False
            )

            print(
                "Final Kafka offsets committed."
            )

    finally:

        consumer.close()

        print(
            "Bronze ingestor stopped cleanly."
        )


if __name__ == "__main__":
    main()