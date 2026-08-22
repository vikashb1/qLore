import os

import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog


# ============================================================
# Iceberg Configuration
# ============================================================

def get_catalog():
    """
    Connect to the qLore Iceberg REST catalog.

    Local Windows execution:
        QLORE_CATALOG_URI defaults to localhost:8181
        QLORE_S3_ENDPOINT defaults to localhost:9000

    Docker / Airflow execution:
        QLORE_CATALOG_URI = http://iceberg-rest:8181
        QLORE_S3_ENDPOINT = http://minio:9000
    """

    catalog_uri = os.getenv(
        "QLORE_CATALOG_URI",
        "http://localhost:8181",
    )

    s3_endpoint = os.getenv(
        "QLORE_S3_ENDPOINT",
        "http://localhost:9000",
    )

    return load_catalog(
        "qlore",
        type="rest",
        uri=catalog_uri,
        warehouse="s3://warehouse",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


# ============================================================
# Bronze Cleaning
# ============================================================

def clean_bronze_telemetry(
    df: pd.DataFrame,
) -> pd.DataFrame:

    print(
        f"\nBronze rows received: "
        f"{len(df)}"
    )

    cleaned = df.copy()

    # --------------------------------------------------------
    # Timestamp normalization
    # --------------------------------------------------------

    cleaned[
        "event_timestamp"
    ] = pd.to_datetime(
        cleaned["event_timestamp"],
        errors="coerce",
    )

    cleaned[
        "ingested_at"
    ] = pd.to_datetime(
        cleaned["ingested_at"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Remove invalid required fields
    # --------------------------------------------------------

    cleaned = cleaned.dropna(
        subset=[
            "device_id",
            "event_timestamp",
            "schema_version",
            "kafka_partition",
            "kafka_offset",
        ]
    )

    # --------------------------------------------------------
    # Standardize text
    # --------------------------------------------------------

    cleaned[
        "device_id"
    ] = (
        cleaned["device_id"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    cleaned[
        "system_status"
    ] = (
        cleaned["system_status"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    cleaned[
        "schema_version"
    ] = (
        cleaned["schema_version"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # --------------------------------------------------------
    # Remove duplicates within Bronze
    # --------------------------------------------------------

    before_dedup = len(
        cleaned
    )

    cleaned = (
        cleaned
        .drop_duplicates(
            subset=[
                "kafka_partition",
                "kafka_offset",
            ],
            keep="last",
        )
    )

    duplicate_count = (
        before_dedup
        - len(cleaned)
    )

    print(
        f"Duplicates removed inside "
        f"Bronze batch: {duplicate_count}"
    )

    # --------------------------------------------------------
    # Validate status
    # --------------------------------------------------------

    valid_statuses = {
        "HEALTHY",
        "DEGRADED",
        "CRITICAL",
    }

    cleaned = cleaned[
        cleaned[
            "system_status"
        ].isin(
            valid_statuses
        )
    ]

    # --------------------------------------------------------
    # Sort deterministically
    # --------------------------------------------------------

    cleaned = (
        cleaned
        .sort_values(
            [
                "event_timestamp",
                "device_id",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    return cleaned


# ============================================================
# Idempotency
# ============================================================

def remove_already_processed_records(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prevent duplicate Silver writes.

    Kafka partition + offset uniquely identify
    each record within the source topic.
    """

    if silver_df.empty:

        print(
            "\nSilver is empty. "
            "All Bronze records are new."
        )

        return bronze_df

    processed_keys = set(
        zip(
            silver_df[
                "kafka_partition"
            ],
            silver_df[
                "kafka_offset"
            ],
        )
    )

    is_new = bronze_df.apply(
        lambda row: (
            row["kafka_partition"],
            row["kafka_offset"],
        )
        not in processed_keys,
        axis=1,
    )

    new_df = bronze_df[
        is_new
    ].copy()

    print(
        f"\nAlready processed rows skipped: "
        f"{len(bronze_df) - len(new_df)}"
    )

    print(
        f"New rows for Silver: "
        f"{len(new_df)}"
    )

    return new_df


# ============================================================
# Pandas → Arrow
# ============================================================

def dataframe_to_arrow(
    df: pd.DataFrame,
) -> pa.Table:

    schema = pa.schema(
        [
            pa.field(
                "device_id",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "event_timestamp",
                pa.timestamp("us"),
                nullable=False,
            ),

            pa.field(
                "temperature_mk",
                pa.float64(),
            ),

            pa.field(
                "cpu_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "memory_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "queue_depth",
                pa.int32(),
            ),

            pa.field(
                "active_jobs",
                pa.int32(),
            ),

            pa.field(
                "signal_noise_ratio",
                pa.float64(),
            ),

            pa.field(
                "cryogenic_pressure_mbar",
                pa.float64(),
            ),

            pa.field(
                "hardware_error_rate",
                pa.float64(),
            ),

            pa.field(
                "system_status",
                pa.string(),
            ),

            pa.field(
                "schema_version",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "ingested_at",
                pa.timestamp("us"),
            ),

            pa.field(
                "kafka_partition",
                pa.int32(),
            ),

            pa.field(
                "kafka_offset",
                pa.int64(),
            ),
        ]
    )

    records = (
        df
        .where(
            pd.notnull(df),
            None,
        )
        .to_dict(
            orient="records"
        )
    )

    return pa.Table.from_pylist(
        records,
        schema=schema,
    )


# ============================================================
# Main Transformation
# ============================================================

def main():

    print(
        "\nqLore Bronze → Silver"
    )

    print("=" * 70)

    catalog = get_catalog()

    bronze_table = catalog.load_table(
        (
            "bronze",
            "telemetry",
        )
    )

    silver_table = catalog.load_table(
        (
            "silver",
            "telemetry",
        )
    )

    # --------------------------------------------------------
    # Read Bronze
    # --------------------------------------------------------

    print(
        "\nReading bronze.telemetry..."
    )

    bronze_df = (
        bronze_table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    if bronze_df.empty:

        print(
            "No Bronze data found."
        )

        return

    # --------------------------------------------------------
    # Clean Bronze
    # --------------------------------------------------------

    cleaned_df = (
        clean_bronze_telemetry(
            bronze_df
        )
    )

    # --------------------------------------------------------
    # Read Silver
    # --------------------------------------------------------

    silver_df = (
        silver_table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"\nCurrent Silver rows: "
        f"{len(silver_df)}"
    )

    # --------------------------------------------------------
    # Keep only unseen Kafka records
    # --------------------------------------------------------

    new_rows = (
        remove_already_processed_records(
            cleaned_df,
            silver_df,
        )
    )

    if new_rows.empty:

        print(
            "\nNo new records to process."
        )

        print(
            "Silver is already up to date."
        )

        return

    # --------------------------------------------------------
    # Write Silver
    # --------------------------------------------------------

    arrow_table = (
        dataframe_to_arrow(
            new_rows
        )
    )

    silver_table.append(
        arrow_table
    )

    print(
        f"\nWrote "
        f"{len(new_rows)} new records "
        f"to silver.telemetry"
    )

    print(
        "\nBronze → Silver "
        "transformation complete."
    )


if __name__ == "__main__":
    main()