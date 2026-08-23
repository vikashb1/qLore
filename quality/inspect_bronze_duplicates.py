import os

from pyiceberg.catalog import load_catalog


CATALOG_URI = os.getenv(
    "QLORE_CATALOG_URI",
    "http://localhost:8181",
)

S3_ENDPOINT = os.getenv(
    "QLORE_S3_ENDPOINT",
    "http://localhost:9000",
)


def get_catalog():

    return load_catalog(
        "qlore",
        type="rest",
        uri=CATALOG_URI,
        warehouse="s3://warehouse",
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def main():

    print("\nqLore Bronze Duplicate Inspection")
    print("=" * 80)

    catalog = get_catalog()

    table = catalog.load_table(
        ("bronze", "telemetry")
    )

    df = (
        table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"\nTotal Bronze rows: {len(df)}"
    )

    duplicate_mask = df.duplicated(
        subset=[
            "kafka_partition",
            "kafka_offset",
        ],
        keep=False,
    )

    duplicates = (
        df[
            duplicate_mask
        ]
        .copy()
        .sort_values(
            [
                "kafka_partition",
                "kafka_offset",
                "event_timestamp",
            ]
        )
    )

    if duplicates.empty:

        print(
            "\nNo duplicate Kafka "
            "partition/offset pairs found."
        )

        return

    duplicate_pairs = (
        duplicates[
            [
                "kafka_partition",
                "kafka_offset",
            ]
        ]
        .drop_duplicates()
    )

    print(
        f"\nDuplicate partition/offset pairs: "
        f"{len(duplicate_pairs)}"
    )

    print(
        f"Rows involved in duplicates: "
        f"{len(duplicates)}"
    )

    print(
        "\nDuplicate records:"
    )

    print("-" * 80)

    columns = [
        "device_id",
        "event_timestamp",
        "schema_version",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    ]

    print(
        duplicates[
            columns
        ].to_string(
            index=False
        )
    )

    print(
        "\nDuplicate pair counts:"
    )

    print("-" * 80)

    counts = (
        duplicates
        .groupby(
            [
                "kafka_partition",
                "kafka_offset",
            ]
        )
        .size()
        .reset_index(
            name="row_count"
        )
    )

    print(
        counts.to_string(
            index=False
        )
    )

    print(
        "\nInterpretation:"
    )

    print(
        "- Same partition/offset + same event_timestamp "
        "likely means replayed ingestion."
    )

    print(
        "- Same partition/offset + different event_timestamp "
        "likely means Kafka offsets were reused after "
        "the topic/broker was recreated."
    )

    print(
        "\n" + "=" * 80
    )


if __name__ == "__main__":
    main()