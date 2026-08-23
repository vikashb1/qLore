import os

import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog


# ============================================================
# Iceberg Configuration
# ============================================================

def get_catalog():
    """
    Connect to qLore Iceberg.

    Local Windows execution defaults to:

        Iceberg REST:
        http://localhost:8181

        MinIO:
        http://localhost:9000

    Airflow running inside Docker receives:

        QLORE_CATALOG_URI=http://iceberg-rest:8181
        QLORE_S3_ENDPOINT=http://minio:9000
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
            "s3.endpoint":
                s3_endpoint,

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


# ============================================================
# Clean Bronze
# ============================================================

def clean_bronze_telemetry(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Standardize and clean Bronze telemetry.

    Event identity is:

        device_id + event_timestamp

    Kafka partition and offset remain as lineage
    attributes, but are not used for deduplication.
    """

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
        cleaned[
            "event_timestamp"
        ],
        errors="coerce",
    )

    cleaned[
        "ingested_at"
    ] = pd.to_datetime(
        cleaned[
            "ingested_at"
        ],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Required fields
    # --------------------------------------------------------

    before_required = len(
        cleaned
    )

    cleaned = (
        cleaned
        .dropna(
            subset=[
                "device_id",
                "event_timestamp",
                "schema_version",
            ]
        )
    )

    removed_missing = (
        before_required
        - len(cleaned)
    )

    print(
        f"Rows removed for missing "
        f"required fields: "
        f"{removed_missing}"
    )

    # --------------------------------------------------------
    # Text standardization
    # --------------------------------------------------------

    cleaned[
        "device_id"
    ] = (
        cleaned[
            "device_id"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    cleaned[
        "system_status"
    ] = (
        cleaned[
            "system_status"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    cleaned[
        "schema_version"
    ] = (
        cleaned[
            "schema_version"
        ]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # --------------------------------------------------------
    # Logical event deduplication
    # --------------------------------------------------------
    #
    # IMPORTANT:
    #
    # We used to deduplicate on:
    #
    #     kafka_partition + kafka_offset
    #
    # That is incorrect across Kafka topic recreation
    # because offsets can restart from zero.
    #
    # qLore now uses:
    #
    #     device_id + event_timestamp
    #
    # as the logical telemetry identity.
    # --------------------------------------------------------

    before_dedup = len(
        cleaned
    )

    cleaned = (
        cleaned
        .drop_duplicates(
            subset=[
                "device_id",
                "event_timestamp",
            ],
            keep="last",
        )
    )

    duplicates_removed = (
        before_dedup
        - len(cleaned)
    )

    print(
        f"Logical duplicate events removed "
        f"inside Bronze: "
        f"{duplicates_removed}"
    )

    # --------------------------------------------------------
    # Status validation
    # --------------------------------------------------------

    valid_statuses = {
        "HEALTHY",
        "DEGRADED",
        "CRITICAL",
    }

    before_status = len(
        cleaned
    )

    cleaned = (
        cleaned[
            cleaned[
                "system_status"
            ].isin(
                valid_statuses
            )
        ]
    )

    invalid_status_removed = (
        before_status
        - len(cleaned)
    )

    print(
        f"Rows removed for invalid status: "
        f"{invalid_status_removed}"
    )

    # --------------------------------------------------------
    # Deterministic ordering
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

    print(
        f"Clean Bronze rows available: "
        f"{len(cleaned)}"
    )

    return cleaned


# ============================================================
# Incremental / Idempotent Processing
# ============================================================

def remove_already_processed_records(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prevent Bronze -> Silver duplication.

    A logical telemetry event is identified by:

        device_id + event_timestamp

    kafka_partition + kafka_offset are retained
    strictly for lineage/debugging.

    This allows the pipeline to remain idempotent
    even if Kafka is recreated and offsets restart.
    """

    if silver_df.empty:

        print(
            "\nSilver is empty."
        )

        print(
            f"All {len(bronze_df)} "
            f"clean Bronze rows are new."
        )

        return bronze_df

    bronze_working = (
        bronze_df.copy()
    )

    silver_working = (
        silver_df.copy()
    )

    # Normalize timestamps on both sides before comparison.

    bronze_working[
        "event_timestamp"
    ] = pd.to_datetime(
        bronze_working[
            "event_timestamp"
        ],
        errors="coerce",
    )

    silver_working[
        "event_timestamp"
    ] = pd.to_datetime(
        silver_working[
            "event_timestamp"
        ],
        errors="coerce",
    )

    bronze_working[
        "device_id"
    ] = (
        bronze_working[
            "device_id"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    silver_working[
        "device_id"
    ] = (
        silver_working[
            "device_id"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # --------------------------------------------------------
    # Build already-processed logical event keys
    # --------------------------------------------------------

    processed_keys = set(
        zip(
            silver_working[
                "device_id"
            ],

            silver_working[
                "event_timestamp"
            ],
        )
    )

    # --------------------------------------------------------
    # Identify unseen logical events
    # --------------------------------------------------------

    is_new = (
        bronze_working
        .apply(
            lambda row: (
                row[
                    "device_id"
                ],
                row[
                    "event_timestamp"
                ],
            )
            not in processed_keys,

            axis=1,
        )
    )

    new_df = (
        bronze_working[
            is_new
        ]
        .copy()
    )

    skipped_count = (
        len(bronze_working)
        - len(new_df)
    )

    print(
        f"\nAlready processed "
        f"telemetry events skipped: "
        f"{skipped_count}"
    )

    print(
        f"New telemetry events "
        f"for Silver: "
        f"{len(new_df)}"
    )

    return new_df


# ============================================================
# Pandas → PyArrow
# ============================================================

def dataframe_to_arrow(
    df: pd.DataFrame,
) -> pa.Table:
    """
    Convert cleaned Silver DataFrame into the
    exact PyArrow schema expected by Iceberg.
    """

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

            # Kafka fields remain because they provide
            # useful source lineage.
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
# Main Bronze → Silver Pipeline
# ============================================================

def main():

    print(
        "\nqLore Bronze → Silver"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Connect to Iceberg
    # --------------------------------------------------------

    catalog = get_catalog()

    bronze_table = (
        catalog.load_table(
            (
                "bronze",
                "telemetry",
            )
        )
    )

    silver_table = (
        catalog.load_table(
            (
                "silver",
                "telemetry",
            )
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

    print(
        f"Current Bronze rows: "
        f"{len(bronze_df)}"
    )

    if bronze_df.empty:

        print(
            "\nNo Bronze data found."
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

    if cleaned_df.empty:

        print(
            "\nNo valid Bronze records "
            "remain after cleaning."
        )

        return

    # --------------------------------------------------------
    # Read current Silver
    # --------------------------------------------------------

    print(
        "\nReading silver.telemetry..."
    )

    silver_df = (
        silver_table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"Current Silver rows: "
        f"{len(silver_df)}"
    )

    # --------------------------------------------------------
    # Determine incremental records
    # --------------------------------------------------------

    new_rows = (
        remove_already_processed_records(
            cleaned_df,
            silver_df,
        )
    )

    # --------------------------------------------------------
    # No-op if Silver is already current
    # --------------------------------------------------------

    if new_rows.empty:

        print(
            "\nNo new records to process."
        )

        print(
            "Silver is already up to date."
        )

        return

    # --------------------------------------------------------
    # Convert to Arrow
    # --------------------------------------------------------

    arrow_table = (
        dataframe_to_arrow(
            new_rows
        )
    )

    # --------------------------------------------------------
    # Append only unseen logical events
    # --------------------------------------------------------

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