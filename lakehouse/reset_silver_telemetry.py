from pyiceberg.catalog import load_catalog


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


def main():

    print("\nqLore Silver Reset")
    print("=" * 70)

    catalog = get_catalog()

    identifier = (
        "silver",
        "telemetry",
    )

    existing_tables = catalog.list_tables(
        "silver"
    )

    if identifier in existing_tables:

        catalog.drop_table(
            identifier
        )

        print(
            "Dropped silver.telemetry"
        )

    else:

        print(
            "silver.telemetry does not exist."
        )

    print(
        "\nRun create_silver_tables.py next."
    )


if __name__ == "__main__":
    main()