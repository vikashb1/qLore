import os

import psycopg2
from pyiceberg.catalog import load_catalog


# ============================================================
# Configuration
# ============================================================

# Local Windows defaults use IPv4 explicitly to avoid
# localhost / IPv6 connection issues.
#
# Airflow running inside Docker can override these through
# environment variables.

CATALOG_URI = os.getenv(
    "QLORE_CATALOG_URI",
    "http://127.0.0.1:8181",
)

S3_ENDPOINT = os.getenv(
    "QLORE_S3_ENDPOINT",
    "http://127.0.0.1:9000",
)

POSTGRES_HOST = os.getenv(
    "QLORE_POSTGRES_HOST",
    "127.0.0.1",
)

POSTGRES_PORT = int(
    os.getenv(
        "QLORE_POSTGRES_PORT",
        "5433",
    )
)

POSTGRES_DB = os.getenv(
    "QLORE_POSTGRES_DB",
    "qlore",
)

POSTGRES_USER = os.getenv(
    "QLORE_POSTGRES_USER",
    "qlore",
)

POSTGRES_PASSWORD = os.getenv(
    "QLORE_POSTGRES_PASSWORD",
    "qlore",
)


# ============================================================
# Iceberg Connection
# ============================================================

def get_catalog():
    """
    Connect to the qLore Iceberg REST catalog.

    Local Windows:
        Catalog:
            http://127.0.0.1:8181

        MinIO:
            http://127.0.0.1:9000

    Docker / Airflow overrides:
        QLORE_CATALOG_URI=http://iceberg-rest:8181
        QLORE_S3_ENDPOINT=http://minio:9000
    """

    return load_catalog(
        "qlore",
        type="rest",
        uri=CATALOG_URI,
        warehouse="s3://warehouse",
        **{
            "s3.endpoint":
                S3_ENDPOINT,

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
# PostgreSQL Connection
# ============================================================

def get_postgres_connection():
    """
    Connect to the qLore operational PostgreSQL database.
    """

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ============================================================
# Convert nullable values safely
# ============================================================

def safe_float(value):

    if value is None:
        return None

    try:
        return float(value)

    except (TypeError, ValueError):
        return None


def safe_int(value):

    if value is None:
        return None

    try:
        return int(value)

    except (TypeError, ValueError):
        return None


# ============================================================
# Main Gold → PostgreSQL Sync
# ============================================================

def main():

    print(
        "\nqLore Gold → PostgreSQL Sync"
    )

    print("=" * 70)

    print(
        f"Iceberg catalog: "
        f"{CATALOG_URI}"
    )

    print(
        f"Object storage:  "
        f"{S3_ENDPOINT}"
    )

    print(
        f"PostgreSQL:      "
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}"
    )

    # --------------------------------------------------------
    # Connect to Iceberg
    # --------------------------------------------------------

    print(
        "\nConnecting to Iceberg..."
    )

    catalog = get_catalog()

    print(
        "Connected to Iceberg catalog."
    )

    # --------------------------------------------------------
    # Load Gold table
    # --------------------------------------------------------

    print(
        "\nLoading gold.device_health..."
    )

    table = catalog.load_table(
        (
            "gold",
            "device_health",
        )
    )

    print(
        "Loaded gold.device_health."
    )

    # --------------------------------------------------------
    # Read Gold into Pandas
    # --------------------------------------------------------

    print(
        "\nReading Gold records..."
    )

    dataframe = (
        table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"Gold device rows received: "
        f"{len(dataframe)}"
    )

    if dataframe.empty:

        print(
            "\nNo Gold records available."
        )

        print(
            "Nothing to synchronize."
        )

        return

    # --------------------------------------------------------
    # Connect to PostgreSQL
    # --------------------------------------------------------

    print(
        "\nConnecting to PostgreSQL..."
    )

    conn = get_postgres_connection()

    cursor = conn.cursor()

    print(
        "Connected to PostgreSQL."
    )

    # --------------------------------------------------------
    # Upsert Statement
    # --------------------------------------------------------

    upsert_sql = """
        INSERT INTO device_health_current (

            device_id,

            total_events,

            avg_temperature_mk,

            avg_cpu_usage_pct,

            avg_memory_usage_pct,

            avg_queue_depth,

            avg_active_jobs,

            avg_signal_noise_ratio,

            avg_cryogenic_pressure_mbar,

            avg_hardware_error_rate,

            healthy_events,

            degraded_events,

            critical_events,

            health_score,

            latest_event_timestamp,

            updated_at
        )

        VALUES (

            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,

            CURRENT_TIMESTAMP
        )

        ON CONFLICT (
            device_id
        )

        DO UPDATE SET

            total_events =
                EXCLUDED.total_events,

            avg_temperature_mk =
                EXCLUDED.avg_temperature_mk,

            avg_cpu_usage_pct =
                EXCLUDED.avg_cpu_usage_pct,

            avg_memory_usage_pct =
                EXCLUDED.avg_memory_usage_pct,

            avg_queue_depth =
                EXCLUDED.avg_queue_depth,

            avg_active_jobs =
                EXCLUDED.avg_active_jobs,

            avg_signal_noise_ratio =
                EXCLUDED.avg_signal_noise_ratio,

            avg_cryogenic_pressure_mbar =
                EXCLUDED.avg_cryogenic_pressure_mbar,

            avg_hardware_error_rate =
                EXCLUDED.avg_hardware_error_rate,

            healthy_events =
                EXCLUDED.healthy_events,

            degraded_events =
                EXCLUDED.degraded_events,

            critical_events =
                EXCLUDED.critical_events,

            health_score =
                EXCLUDED.health_score,

            latest_event_timestamp =
                EXCLUDED.latest_event_timestamp,

            updated_at =
                CURRENT_TIMESTAMP;
    """

    synced = 0

    # --------------------------------------------------------
    # Upsert each Gold record
    # --------------------------------------------------------

    for _, row in dataframe.iterrows():

        cursor.execute(
            upsert_sql,
            (
                str(
                    row[
                        "device_id"
                    ]
                ),

                safe_int(
                    row[
                        "total_events"
                    ]
                ),

                safe_float(
                    row[
                        "avg_temperature_mk"
                    ]
                ),

                safe_float(
                    row[
                        "avg_cpu_usage_pct"
                    ]
                ),

                safe_float(
                    row[
                        "avg_memory_usage_pct"
                    ]
                ),

                safe_float(
                    row[
                        "avg_queue_depth"
                    ]
                ),

                safe_float(
                    row[
                        "avg_active_jobs"
                    ]
                ),

                safe_float(
                    row[
                        "avg_signal_noise_ratio"
                    ]
                ),

                safe_float(
                    row[
                        "avg_cryogenic_pressure_mbar"
                    ]
                ),

                safe_float(
                    row[
                        "avg_hardware_error_rate"
                    ]
                ),

                safe_int(
                    row[
                        "healthy_events"
                    ]
                ),

                safe_int(
                    row[
                        "degraded_events"
                    ]
                ),

                safe_int(
                    row[
                        "critical_events"
                    ]
                ),

                safe_float(
                    row[
                        "health_score"
                    ]
                ),

                row[
                    "latest_event_timestamp"
                ],
            ),
        )

        synced += 1

    # --------------------------------------------------------
    # Commit PostgreSQL transaction
    # --------------------------------------------------------

    conn.commit()

    print(
        f"\nDevice health rows synchronized: "
        f"{synced}"
    )

    # --------------------------------------------------------
    # Verify PostgreSQL records
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT

            device_id,

            total_events,

            health_score,

            latest_event_timestamp,

            updated_at

        FROM device_health_current

        ORDER BY device_id;
        """
    )

    rows = cursor.fetchall()

    print(
        "\nCurrent PostgreSQL health state:"
    )

    print("-" * 70)

    for row in rows:

        device_id = row[0]

        total_events = row[1]

        health_score = row[2]

        latest_timestamp = row[3]

        updated_at = row[4]

        if health_score is None:

            health_text = "NULL"

        else:

            health_text = (
                f"{health_score:.2f}"
            )

        print(
            f"{device_id} | "
            f"events={total_events} | "
            f"health={health_text} | "
            f"latest={latest_timestamp} | "
            f"updated={updated_at}"
        )

    # --------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------

    cursor.close()

    conn.close()

    print(
        "\nGold → PostgreSQL "
        "synchronization complete."
    )

    print("=" * 70)


if __name__ == "__main__":

    main()