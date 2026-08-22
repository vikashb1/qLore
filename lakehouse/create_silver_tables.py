from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
)


def get_catalog():

    return load_catalog(
        "qlore",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://warehouse",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def silver_telemetry_schema():

    return Schema(

        NestedField(
            1,
            "device_id",
            StringType(),
            required=True,
        ),

        NestedField(
            2,
            "event_timestamp",
            TimestampType(),
            required=True,
        ),

        NestedField(
            3,
            "temperature_mk",
            DoubleType(),
            required=False,
        ),

        NestedField(
            4,
            "cpu_usage_pct",
            DoubleType(),
            required=False,
        ),

        NestedField(
            5,
            "memory_usage_pct",
            DoubleType(),
            required=False,
        ),

        NestedField(
            6,
            "queue_depth",
            IntegerType(),
            required=False,
        ),

        NestedField(
            7,
            "active_jobs",
            IntegerType(),
            required=False,
        ),

        NestedField(
            8,
            "signal_noise_ratio",
            DoubleType(),
            required=False,
        ),

        NestedField(
            9,
            "cryogenic_pressure_mbar",
            DoubleType(),
            required=False,
        ),

        NestedField(
            10,
            "hardware_error_rate",
            DoubleType(),
            required=False,
        ),

        NestedField(
            11,
            "system_status",
            StringType(),
            required=False,
        ),

        NestedField(
            12,
            "schema_version",
            StringType(),
            required=True,
        ),

        NestedField(
            13,
            "ingested_at",
            TimestampType(),
            required=False,
        ),

        NestedField(
            14,
            "kafka_partition",
            IntegerType(),
            required=False,
        ),

        NestedField(
            15,
            "kafka_offset",
            LongType(),
            required=False,
        ),
    )


def main():

    print("\nqLore Silver Table Setup")
    print("=" * 70)

    catalog = get_catalog()

    identifier = (
        "silver",
        "telemetry",
    )

    existing = catalog.list_tables(
        "silver"
    )

    if identifier in existing:

        print(
            "silver.telemetry already exists."
        )

        table = catalog.load_table(
            identifier
        )

    else:

        table = catalog.create_table(
            identifier=identifier,
            schema=silver_telemetry_schema(),
        )

        print(
            "Created Iceberg table: "
            "silver.telemetry"
        )

    print("\nSchema:")
    print(table.schema())

    print("\nLocation:")
    print(table.location())


if __name__ == "__main__":
    main()