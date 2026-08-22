import json

from confluent_kafka import (
    Consumer,
    KafkaError,
)


BOOTSTRAP_SERVERS = (
    "localhost:9092"
)

TOPIC = "qlore.jobs.v1"

CONSUMER_GROUP = (
    "qlore-job-consumers"
)


def main():

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            "auto.offset.reset":
                "earliest",

            "enable.auto.commit":
                True,
        }
    )

    consumer.subscribe(
        [TOPIC]
    )

    print(
        "\nqLore Job Consumer"
    )

    print("=" * 80)

    print(
        f"Topic: {TOPIC}"
    )

    print(
        f"Consumer group: "
        f"{CONSUMER_GROUP}"
    )

    print(
        "\nWaiting for job events..."
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
                f"Job: "
                f"{value['job_id']}"
            )

            print(
                f"Status: "
                f"{value['status']}"
            )

            print(
                f"Error: "
                f"{value['error_code']}"
            )

            print(
                f"Queue: "
                f"{value['queue_time_ms']} ms"
            )

            print(
                f"Execution: "
                f"{value['execution_time_ms']} ms"
            )

            print(
                f"Schema: "
                f"{value['schema_version']}"
            )

    except KeyboardInterrupt:

        print(
            "\nStopping job consumer..."
        )

    finally:

        consumer.close()

        print(
            "Job consumer stopped."
        )


if __name__ == "__main__":
    main()