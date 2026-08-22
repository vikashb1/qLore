import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "qlore.jobs.v1"

RANDOM_SEED = 46


def load_devices() -> pd.DataFrame:

    path = Path(
        "data/generated/devices.csv"
    )

    if not path.exists():
        raise FileNotFoundError(
            "devices.csv not found."
        )

    return pd.read_csv(path)


def delivery_report(
    err,
    msg,
):

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


def generate_job_event(
    device_id: str,
    rng: np.random.Generator,
) -> dict:

    queue_time_ms = int(
        rng.lognormal(
            mean=7.5,
            sigma=0.7,
        )
    )

    execution_time_ms = int(
        rng.lognormal(
            mean=6.5,
            sigma=0.6,
        )
    )

    shots = int(
        rng.choice(
            [
                100,
                500,
                1000,
                2000,
                4000,
            ]
        )
    )

    circuit_depth = int(
        rng.integers(
            5,
            250,
        )
    )

    # Normal devices around ~4% failure
    failure_probability = 0.04

    # Slightly elevated live failure rate
    # for our known degraded device
    if device_id == "DEV002":
        failure_probability = 0.12

    failed = (
        rng.random()
        < failure_probability
    )

    if failed:

        status = "FAILED"

        error_code = rng.choice(
            [
                "CALIBRATION_ERROR",
                "EXECUTION_ERROR",
                "DEVICE_ERROR",
                "TIMEOUT",
            ],
            p=[
                0.50,
                0.20,
                0.20,
                0.10,
            ],
        )

    else:

        status = "SUCCESS"
        error_code = None

    submitted_at = datetime.now(
        timezone.utc
    )

    event = {
        "job_id": (
            f"LIVE-{submitted_at.timestamp()}-"
            f"{rng.integers(1000, 9999)}"
        ),

        "device_id": device_id,

        "user_id": (
            f"USR{rng.integers(1, 5001):05d}"
        ),

        "submitted_at": (
            submitted_at.isoformat()
        ),

        "queue_time_ms": (
            queue_time_ms
        ),

        "execution_time_ms": (
            execution_time_ms
        ),

        "shots": shots,

        "circuit_depth": (
            circuit_depth
        ),

        "status": status,

        "error_code": (
            str(error_code)
            if error_code
            else None
        ),

        "schema_version": "v1",
    }

    return event


def main():

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

            "acks": "all",
        }
    )

    print(
        "\nqLore Job Event Producer"
    )

    print("=" * 70)

    print(
        f"Topic: {TOPIC}"
    )

    print(
        "\nStreaming job events..."
    )

    try:

        while True:

            device_id = rng.choice(
                device_ids
            )

            event = generate_job_event(
                device_id,
                rng,
            )

            producer.produce(
                topic=TOPIC,

                key=(
                    device_id.encode(
                        "utf-8"
                    )
                ),

                value=(
                    json.dumps(
                        event
                    ).encode(
                        "utf-8"
                    )
                ),

                callback=delivery_report,
            )

            producer.poll(0)

            time.sleep(
                rng.uniform(
                    0.3,
                    1.0,
                )
            )

    except KeyboardInterrupt:

        print(
            "\nStopping job producer..."
        )

    finally:

        producer.flush()

        print(
            "Job producer stopped."
        )


if __name__ == "__main__":
    main()