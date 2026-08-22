from pyiceberg.catalog import load_catalog


# ============================================================
# Iceberg Configuration
# ============================================================

CATALOG_URI = "http://localhost:8181"
WAREHOUSE = "s3://warehouse"


def get_catalog():
    """
    Connect to the qLore Iceberg REST catalog.
    """

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

    print("\nqLore Bronze Telemetry Verification")
    print("=" * 70)

    # --------------------------------------------------------
    # Connect to Iceberg
    # --------------------------------------------------------

    print("\nConnecting to Iceberg...")

    catalog = get_catalog()

    print("Connected.")

    # --------------------------------------------------------
    # Load Bronze table
    # --------------------------------------------------------

    print("\nLoading bronze.telemetry...")

    table = catalog.load_table(
        ("bronze", "telemetry")
    )

    print("Loaded bronze.telemetry.")

    # --------------------------------------------------------
    # Read Iceberg data
    # --------------------------------------------------------

    print("\nScanning Iceberg table...")

    arrow_table = (
        table
        .scan()
        .to_arrow()
    )

    print(
        f"\nTotal Bronze records: "
        f"{arrow_table.num_rows}"
    )

    # --------------------------------------------------------
    # Display schema
    # --------------------------------------------------------

    print("\nSchema:")
    print("-" * 70)

    print(
        arrow_table.schema
    )

    # --------------------------------------------------------
    # Display records
    # --------------------------------------------------------

    print("\nFirst 10 Bronze records:")
    print("-" * 70)

    if arrow_table.num_rows == 0:

        print(
            "No records found in bronze.telemetry."
        )

    else:

        dataframe = (
            arrow_table
            .slice(0, 10)
            .to_pandas()
        )

        print(
            dataframe.to_string(
                index=False
            )
        )

    # --------------------------------------------------------
    # Show schema versions
    # --------------------------------------------------------

    if (
        arrow_table.num_rows > 0
        and "schema_version"
        in arrow_table.column_names
    ):

        dataframe = (
            arrow_table
            .to_pandas()
        )

        print("\nSchema Version Counts:")
        print("-" * 70)

        print(
            dataframe[
                "schema_version"
            ].value_counts(
                dropna=False
            )
        )

    # --------------------------------------------------------
    # Show Kafka lineage
    # --------------------------------------------------------

    if arrow_table.num_rows > 0:

        dataframe = (
            arrow_table
            .to_pandas()
        )

        print("\nKafka Partition / Offset Sample:")
        print("-" * 70)

        columns = [
            "device_id",
            "kafka_partition",
            "kafka_offset",
            "ingested_at",
        ]

        available_columns = [
            column
            for column in columns
            if column in dataframe.columns
        ]

        print(
            dataframe[
                available_columns
            ]
            .head(10)
            .to_string(
                index=False
            )
        )

    print("\n" + "=" * 70)

    print(
        "Bronze telemetry verification complete."
    )

    print("=" * 70)


if __name__ == "__main__":
    main()