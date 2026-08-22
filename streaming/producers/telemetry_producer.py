import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "qlore.telemetry.v1"
RANDOM_SEED = 45


def load_devices() -> pd.DataFrame:
    """
    Load the qLore device registry.
    """

    path = Path("data/generated/devices.csv")

    if not path.exists():
        raise FileNotFoundError(
            "devices.csv not found. "
            "Run device_generator.py first."
        )

    return pd.read_csv(path)


def delivery_report(err, msg):
    """
    Kafka delivery callback.

    Called after Kafka either successfully delivers
    a message or encounters a delivery failure.
    """

    if err is not None:
        print(
            f"Delivery failed: {err}"
        )
        return

    print(
        f"Delivered "
        f"key={msg.key().decode('utf-8')} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def generate_event(
    device_id: str,
    rng: np.random.Generator,
) -> dict:
    """
    Generate one qLore telemetry V2 event.

    V2 extends the original telemetry schema with:

    - cryogenic_pressure_mbar
    - hardware_error_rate
    """

    # --------------------------------------------------
    # Core telemetry fields
    # --------------------------------------------------

    temperature = rng.normal(
        loc=15.0,
        scale=1.0,
    )

    cpu_usage = np.clip(
        rng.normal(
            loc=45,
            scale=12,
        ),
        0,
        100,
    )

    memory_usage = np.clip(
        rng.normal(
            loc=52,
            scale=10,
        ),
        0,
        100,
    )

    queue_depth = max(
        0,
        int(
            rng.normal(
                loc=35,
                scale=15,
            )
        ),
    )

    active_jobs = max(
        0,
        int(
            queue_depth
            * rng.uniform(
                0.2,
                0.6,
            )
        ),
    )

    signal_noise_ratio = max(
        0,
        rng.normal(
            loc=30,
            scale=2,
        ),
    )

    # --------------------------------------------------
    # V2 telemetry fields
    # --------------------------------------------------

    cryogenic_pressure = max(
        0,
        rng.normal(
            loc=1.05,
            scale=0.05,
        ),
    )

    hardware_error_rate = max(
        0,
        rng.normal(
            loc=0.002,
            scale=0.0005,
        ),
    )

    # --------------------------------------------------
    # Determine system health
    # --------------------------------------------------

    system_status = "HEALTHY"

    if (
        temperature > 22
        or signal_noise_ratio < 20
        or queue_depth > 90
        or hardware_error_rate > 0.008
    ):
        system_status = "DEGRADED"

    if (
        temperature > 28
        or signal_noise_ratio < 15
        or queue_depth > 130
        or hardware_error_rate > 0.015
    ):
        system_status = "CRITICAL"

    # --------------------------------------------------
    # Construct V2 event
    # --------------------------------------------------

    event = {
        "device_id": device_id,

        "event_timestamp": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "temperature_mk": round(
            float(temperature),
            3,
        ),

        "cpu_usage_pct": round(
            float(cpu_usage),
            2,
        ),

        "memory_usage_pct": round(
            float(memory_usage),
            2,
        ),

        "queue_depth": queue_depth,

        "active_jobs": active_jobs,

        "signal_noise_ratio": round(
            float(signal_noise_ratio),
            3,
        ),

        # Added in schema V2
        "cryogenic_pressure_mbar": round(
            float(cryogenic_pressure),
            4,
        ),

        # Added in schema V2
        "hardware_error_rate": round(
            float(hardware_error_rate),
            6,
        ),

        "system_status": system_status,

        "schema_version": "v2",
    }

    return event


def main():
    """
    Start the continuous qLore telemetry producer.
    """

    devices_df = load_devices()

    device_ids = (
        devices_df[
            "device_id"
        ].tolist()
    )

    rng = np.random.default_rng(
        RANDOM_SEED
    )

    producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            # Wait for acknowledgement from
            # all required in-sync replicas.
            "acks": "all",
        }
    )

    print(
        "\nqLore Telemetry Producer V2"
    )

    print("=" * 70)

    print(
        f"Kafka topic: {TOPIC}"
    )

    print(
        f"Devices: {device_ids}"
    )

    print(
        "Schema version: v2"
    )

    print(
        "\nStreaming telemetry..."
    )

    try:

        while True:

            for device_id in device_ids:

                event = generate_event(
                    device_id,
                    rng,
                )

                # device_id becomes the Kafka message key.
                #
                # This keeps events belonging to the same
                # device on the same Kafka partition.
                message_key = (
                    device_id.encode(
                        "utf-8"
                    )
                )

                message_value = (
                    json.dumps(
                        event
                    ).encode(
                        "utf-8"
                    )
                )

                producer.produce(
                    topic=TOPIC,
                    key=message_key,
                    value=message_value,
                    callback=delivery_report,
                )

                # Trigger delivery callbacks.
                producer.poll(0)

            # Ensure this batch has reached Kafka.
            producer.flush()

            time.sleep(2)

    except KeyboardInterrupt:

        print(
            "\nStopping qLore telemetry producer..."
        )

    finally:

        producer.flush()

        print(
            "Producer stopped cleanly."
        )


if __name__ == "__main__":
    main()