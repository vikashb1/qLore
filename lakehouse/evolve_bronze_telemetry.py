from pyiceberg.catalog import load_catalog
from pyiceberg.types import (
    TimestampType,
    IntegerType,
    LongType,
)


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


def main():

    print("\nqLore Bronze Schema Evolution")
    print("=" * 70)

    catalog = get_catalog()

    table = catalog.load_table(
        ("bronze", "telemetry")
    )

    print("\nCurrent schema:")
    print(table.schema())

    existing_columns = {
        field.name
        for field in table.schema().fields
    }

    with table.update_schema() as update:

        if "ingested_at" not in existing_columns:

            update.add_column(
                "ingested_at",
                TimestampType(),
            )

            print(
                "\nAdding column: ingested_at"
            )

        if "kafka_partition" not in existing_columns:

            update.add_column(
                "kafka_partition",
                IntegerType(),
            )

            print(
                "Adding column: kafka_partition"
            )

        if "kafka_offset" not in existing_columns:

            update.add_column(
                "kafka_offset",
                LongType(),
            )

            print(
                "Adding column: kafka_offset"
            )

    table.refresh()

    print("\nUpdated schema:")
    print(table.schema())

    print(
        "\nBronze schema evolution complete."
    )


if __name__ == "__main__":
    main()