import os

import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog


# ============================================================
# Iceberg Configuration
# ============================================================

def get_catalog():
    """
    Connect to Iceberg in either local Windows mode
    or Docker/Airflow mode.
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
# Health Score
# ============================================================

def calculate_health_score(
    row,
):

    total = row[
        "total_events"
    ]

    if total == 0:
        return 0.0

    healthy_ratio = (
        row[
            "healthy_events"
        ]
        / total
    )

    degraded_ratio = (
        row[
            "degraded_events"
        ]
        / total
    )

    critical_ratio = (
        row[
            "critical_events"
        ]
        / total
    )

    score = (
        healthy_ratio * 100
        - degraded_ratio * 25
        - critical_ratio * 75
    )

    return round(
        max(
            0.0,
            min(
                100.0,
                score,
            ),
        ),
        2,
    )


# ============================================================
# Silver → Gold Aggregation
# ============================================================

def build_device_health(
    df: pd.DataFrame,
) -> pd.DataFrame:

    print(
        f"\nSilver rows received: "
        f"{len(df)}"
    )

    working = df.copy()

    # --------------------------------------------------------
    # Status flags
    # --------------------------------------------------------

    working[
        "healthy_flag"
    ] = (
        working[
            "system_status"
        ]
        == "HEALTHY"
    ).astype(int)

    working[
        "degraded_flag"
    ] = (
        working[
            "system_status"
        ]
        == "DEGRADED"
    ).astype(int)

    working[
        "critical_flag"
    ] = (
        working[
            "system_status"
        ]
        == "CRITICAL"
    ).astype(int)

    # --------------------------------------------------------
    # Device aggregation
    # --------------------------------------------------------

    gold = (
        working
        .groupby(
            "device_id",
            as_index=False,
        )
        .agg(

            total_events=(
                "device_id",
                "size",
            ),

            avg_temperature_mk=(
                "temperature_mk",
                "mean",
            ),

            avg_cpu_usage_pct=(
                "cpu_usage_pct",
                "mean",
            ),

            avg_memory_usage_pct=(
                "memory_usage_pct",
                "mean",
            ),

            avg_queue_depth=(
                "queue_depth",
                "mean",
            ),

            avg_active_jobs=(
                "active_jobs",
                "mean",
            ),

            avg_signal_noise_ratio=(
                "signal_noise_ratio",
                "mean",
            ),

            avg_cryogenic_pressure_mbar=(
                "cryogenic_pressure_mbar",
                "mean",
            ),

            avg_hardware_error_rate=(
                "hardware_error_rate",
                "mean",
            ),

            healthy_events=(
                "healthy_flag",
                "sum",
            ),

            degraded_events=(
                "degraded_flag",
                "sum",
            ),

            critical_events=(
                "critical_flag",
                "sum",
            ),

            latest_event_timestamp=(
                "event_timestamp",
                "max",
            ),
        )
    )

    # --------------------------------------------------------
    # Health score
    # --------------------------------------------------------

    gold[
        "health_score"
    ] = (
        gold.apply(
            calculate_health_score,
            axis=1,
        )
    )

    # --------------------------------------------------------
    # Round metrics
    # --------------------------------------------------------

    numeric_columns = [
        "avg_temperature_mk",
        "avg_cpu_usage_pct",
        "avg_memory_usage_pct",
        "avg_queue_depth",
        "avg_active_jobs",
        "avg_signal_noise_ratio",
        "avg_cryogenic_pressure_mbar",
        "avg_hardware_error_rate",
    ]

    gold[
        numeric_columns
    ] = (
        gold[
            numeric_columns
        ]
        .round(6)
    )

    gold = (
        gold
        .sort_values(
            "device_id"
        )
        .reset_index(
            drop=True
        )
    )

    print(
        f"Gold device rows produced: "
        f"{len(gold)}"
    )

    return gold


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
                "total_events",
                pa.int64(),
                nullable=False,
            ),

            pa.field(
                "avg_temperature_mk",
                pa.float64(),
            ),

            pa.field(
                "avg_cpu_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "avg_memory_usage_pct",
                pa.float64(),
            ),

            pa.field(
                "avg_queue_depth",
                pa.float64(),
            ),

            pa.field(
                "avg_active_jobs",
                pa.float64(),
            ),

            pa.field(
                "avg_signal_noise_ratio",
                pa.float64(),
            ),

            pa.field(
                "avg_cryogenic_pressure_mbar",
                pa.float64(),
            ),

            pa.field(
                "avg_hardware_error_rate",
                pa.float64(),
            ),

            pa.field(
                "healthy_events",
                pa.int64(),
                nullable=False,
            ),

            pa.field(
                "degraded_events",
                pa.int64(),
                nullable=False,
            ),

            pa.field(
                "critical_events",
                pa.int64(),
                nullable=False,
            ),

            pa.field(
                "health_score",
                pa.float64(),
                nullable=False,
            ),

            pa.field(
                "latest_event_timestamp",
                pa.timestamp("us"),
                nullable=False,
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
# Main
# ============================================================

def main():

    print(
        "\nqLore Silver → Gold Device Health"
    )

    print("=" * 70)

    catalog = get_catalog()

    silver_table = (
        catalog.load_table(
            (
                "silver",
                "telemetry",
            )
        )
    )

    gold_table = (
        catalog.load_table(
            (
                "gold",
                "device_health",
            )
        )
    )

    print(
        "\nReading silver.telemetry..."
    )

    silver_df = (
        silver_table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    if silver_df.empty:

        print(
            "No Silver data found."
        )

        return

    gold_df = (
        build_device_health(
            silver_df
        )
    )

    arrow_table = (
        dataframe_to_arrow(
            gold_df
        )
    )

    # Current-state Gold aggregate.
    # Rebuild each time instead of appending duplicates.
    gold_table.overwrite(
        arrow_table
    )

    print(
        f"\nWrote "
        f"{len(gold_df)} "
        f"device records to "
        f"gold.device_health"
    )

    print(
        "\nSilver → Gold "
        "transformation complete."
    )


if __name__ == "__main__":
    main()