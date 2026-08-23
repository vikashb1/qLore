import json
import time
import random
from datetime import datetime, timezone

from confluent_kafka import Producer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "qlore.telemetry.v1"

DEVICE_ID = "DEV002"

# Generate enough events to make the degradation visible
EVENT_COUNT = 10
EVENT_INTERVAL_SECONDS = 0.5


# ============================================================
# Kafka Delivery Callback
# ============================================================

def delivery_report(err, msg):

    if err is not None:
        print(
            f"Delivery failed: {err}"
        )
        return

    print(
        f"Delivered DEV002 degradation event "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


# ============================================================
# Generate Degraded Telemetry
# ============================================================

def generate_degraded_event():

    """
    Generate intentionally unhealthy telemetry for DEV002.

    Values are designed to trigger qLore anomaly rules:

    temperature_mk > 18
    hardware_error_rate > 0.005
    queue_depth > 80
    signal_noise_ratio < 20
    system_status = DEGRADED
    """

    event = {

        "device_id": DEVICE_ID,

        "event_timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "temperature_mk":
            round(
                random.uniform(
                    20.0,
                    23.0,
                ),
                3,
            ),

        "cpu_usage_pct":
            round(
                random.uniform(
                    82.0,
                    96.0,
                ),
                2,
            ),

        "memory_usage_pct":
            round(
                random.uniform(
                    80.0,
                    94.0,
                ),
                2,
            ),

        "queue_depth":
            random.randint(
                90,
                120,
            ),

        "active_jobs":
            random.randint(
                25,
                45,
            ),

        "signal_noise_ratio":
            round(
                random.uniform(
                    12.0,
                    18.0,
                ),
                3,
            ),

        "gate_fidelity":
            round(
                random.uniform(
                    0.9300,
                    0.9700,
                ),
                4,
            ),

        "hardware_error_rate":
            round(
                random.uniform(
                    0.008,
                    0.015,
                ),
                6,
            ),

        "system_status":
            "DEGRADED",

        "schema_version":
            "v2",
    }

    return event


# ============================================================
# Main
# ============================================================

def main():

    print(
        "\nqLore Controlled Degradation Generator"
    )

    print("=" * 70)

    print(
        f"Kafka topic: {KAFKA_TOPIC}"
    )

    print(
        f"Target device: {DEVICE_ID}"
    )

    print(
        f"Events: {EVENT_COUNT}"
    )

    print()

    print(
        "This test intentionally generates abnormal telemetry."
    )

    print()

    producer = Producer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "client.id":
                "qlore-degradation-generator",

            "acks":
                "all",
        }
    )

    print(
        "Generating controlled DEV002 degradation..."
    )

    print("-" * 70)

    for number in range(
        1,
        EVENT_COUNT + 1,
    ):

        event = (
            generate_degraded_event()
        )

        payload = json.dumps(
            event
        )

        producer.produce(
            topic=KAFKA_TOPIC,
            key=DEVICE_ID,
            value=payload,
            callback=delivery_report,
        )

        producer.poll(0)

        print(
            f"[{number:02d}/{EVENT_COUNT}] "
            f"DEV002 | "
            f"temp={event['temperature_mk']}mK | "
            f"cpu={event['cpu_usage_pct']}% | "
            f"memory={event['memory_usage_pct']}% | "
            f"queue={event['queue_depth']} | "
            f"snr={event['signal_noise_ratio']} | "
            f"error={event['hardware_error_rate']} | "
            f"status={event['system_status']}"
        )

        time.sleep(
            EVENT_INTERVAL_SECONDS
        )

    print(
        "\nFlushing Kafka producer..."
    )

    remaining = producer.flush(
        15
    )

    if remaining == 0:

        print(
            "All degradation events delivered successfully."
        )

    else:

        print(
            f"WARNING: {remaining} message(s) "
            f"were not delivered."
        )

    print()

    print("=" * 70)

    print(
        "Controlled DEV002 degradation generation complete."
    )

    print("=" * 70)


if __name__ == "__main__":
    main()