import json

from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "qlore.telemetry.v1"

# Temporary group for the schema-evolution demo.
# Because this is a new group, Kafka will read from the beginning.
CONSUMER_GROUP = "qlore-schema-demo-consumers"


def main():

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe(
        [TOPIC]
    )

    print(
        "\nqLore Telemetry Consumer"
    )

    print("=" * 80)

    print(
        f"Topic: {TOPIC}"
    )

    print(
        f"Consumer group: {CONSUMER_GROUP}"
    )

    print(
        "\nWaiting for telemetry events..."
    )

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

            key = (
                message.key()
                .decode("utf-8")
                if message.key()
                else None
            )

            value = json.loads(
                message.value()
                .decode("utf-8")
            )

            # -----------------------------------------
            # Backward-compatible schema handling
            # -----------------------------------------

            schema_version = value.get(
                "schema_version",
                "v1",
            )

            cryogenic_pressure = value.get(
                "cryogenic_pressure_mbar"
            )

            hardware_error_rate = value.get(
                "hardware_error_rate"
            )

            print(
                "\n" + "-" * 80
            )

            print(
                f"Device: {key}"
            )

            print(
                f"Partition: "
                f"{message.partition()}"
            )

            print(
                f"Offset: "
                f"{message.offset()}"
            )

            print(
                f"Timestamp: "
                f"{value.get('event_timestamp')}"
            )

            print(
                f"Temperature: "
                f"{value.get('temperature_mk')} mK"
            )

            print(
                f"Queue depth: "
                f"{value.get('queue_depth')}"
            )

            print(
                f"SNR: "
                f"{value.get('signal_noise_ratio')}"
            )

            print(
                f"Status: "
                f"{value.get('system_status')}"
            )

            print(
                f"Schema: "
                f"{schema_version}"
            )

            # -----------------------------------------
            # Fields introduced in schema V2
            # -----------------------------------------

            if schema_version == "v2":

                print(
                    f"Cryogenic pressure: "
                    f"{cryogenic_pressure} mbar"
                )

                print(
                    f"Hardware error rate: "
                    f"{hardware_error_rate}"
                )

    except KeyboardInterrupt:

        print(
            "\nStopping consumer..."
        )

    finally:

        consumer.close()

        print(
            "Consumer stopped cleanly."
        )


if __name__ == "__main__":
    main()