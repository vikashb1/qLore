import os

from pyiceberg.catalog import load_catalog

from quality.telemetry_quality import (
    run_telemetry_quality_checks,
)


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

    print(
        "\nqLore Bronze Data Quality"
    )

    print("=" * 70)

    print(
        f"Catalog: {CATALOG_URI}"
    )

    print(
        f"Object storage: {S3_ENDPOINT}"
    )

    catalog = get_catalog()

    table = catalog.load_table(
        (
            "bronze",
            "telemetry",
        )
    )

    dataframe = (
        table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"\nRows scanned: "
        f"{len(dataframe)}"
    )

    results = (
        run_telemetry_quality_checks(
            dataframe
        )
    )

    print(
        "\nQuality Check Results"
    )

    print("-" * 70)

    for (
        check_name,
        result,
    ) in results.items():

        if check_name == "overall":
            continue

        status = (
            "PASS"
            if result["passed"]
            else "FAIL"
        )

        print(
            f"{check_name:<20} "
            f"{status}"
        )

        for error in result["errors"]:
            print(
                f"  - {error}"
            )

    print(
        "\n" + "=" * 70
    )

    if results["overall"]["passed"]:

        print(
            "OVERALL DATA QUALITY: PASS"
        )

    else:

        print(
            "OVERALL DATA QUALITY: FAIL"
        )

        print(
            f"Total errors: "
            f"{results['overall']['error_count']}"
        )

        # Important for Airflow:
        # non-zero exit code makes the task FAIL.
        raise RuntimeError(
            "Bronze telemetry failed "
            "data-quality validation."
        )

    print(
        "=" * 70
    )


if __name__ == "__main__":
    main()