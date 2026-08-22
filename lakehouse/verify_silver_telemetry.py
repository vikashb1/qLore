from pyiceberg.catalog import load_catalog


def get_catalog():

    return load_catalog(
        "qlore",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://warehouse",
        **{
            "s3.endpoint":
                "http://localhost:9000",

            "s3.access-key-id":
                "admin",

            "s3.secret-access-key":
                "password",

            "s3.region":
                "us-east-1",

            "s3.path-style-access":
                "true",
        },
    )


def main():

    print(
        "\nqLore Silver Verification"
    )

    print("=" * 70)

    catalog = get_catalog()

    table = catalog.load_table(
        (
            "silver",
            "telemetry",
        )
    )

    arrow_table = (
        table
        .scan()
        .to_arrow()
    )

    print(
        f"\nTotal Silver records: "
        f"{arrow_table.num_rows}"
    )

    if arrow_table.num_rows > 0:

        df = (
            arrow_table
            .to_pandas()
        )

        print(
            "\nStatus distribution:"
        )

        print(
            df[
                "system_status"
            ].value_counts()
        )

        print(
            "\nDevice distribution:"
        )

        print(
            df[
                "device_id"
            ].value_counts()
        )

        print(
            "\nFirst 10 records:"
        )

        print(
            df
            .head(10)
            .to_string(
                index=False
            )
        )

    print(
        "\nSilver verification complete."
    )


if __name__ == "__main__":
    main()