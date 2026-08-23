import hashlib
import os
from datetime import datetime, timezone

import psycopg2
from pyiceberg.catalog import load_catalog


# ============================================================
# Configuration
# ============================================================

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
# Anomaly Thresholds
# ============================================================

HIGH_TEMPERATURE_MK = 18.0

HIGH_HARDWARE_ERROR_RATE = 0.005

HIGH_QUEUE_DEPTH = 80

LOW_SIGNAL_NOISE_RATIO = 20.0


# ============================================================
# Iceberg
# ============================================================

def get_catalog():
    """
    Connect to qLore Iceberg.

    Local Windows:
        127.0.0.1

    Airflow / Docker:
        overridden with environment variables.
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
# PostgreSQL
# ============================================================

def get_postgres_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ============================================================
# Deterministic Event Key
# ============================================================

def build_event_key(
    device_id,
    event_timestamp,
    incident_type,
):
    """
    Create a deterministic identifier for one anomaly.

    Example logical identity:

        DEV002
        + telemetry timestamp
        + HARDWARE_ERROR_SPIKE

    Running the detector multiple times therefore
    produces the same event_key and PostgreSQL can
    safely skip duplicate incidents.
    """

    raw = (
        f"{device_id}|"
        f"{event_timestamp}|"
        f"{incident_type}"
    )

    digest = hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:24]

    return (
        f"EVT-{digest.upper()}"
    )


# ============================================================
# Incident Builder
# ============================================================

def create_incident(
    row,
    incident_type,
    severity,
    description,
):

    event_timestamp = (
        row["event_timestamp"]
    )

    return {

        "event_key":
            build_event_key(
                row["device_id"],
                event_timestamp,
                incident_type,
            ),

        "device_id":
            str(
                row["device_id"]
            ),

        "incident_type":
            incident_type,

        "severity":
            severity,

        "description":
            description,

        "event_timestamp":
            event_timestamp,
    }


# ============================================================
# Detection Rules
# ============================================================

def detect_row_anomalies(
    row,
):

    incidents = []

    device_id = str(
        row["device_id"]
    )

    temperature = (
        row["temperature_mk"]
    )

    hardware_error_rate = (
        row["hardware_error_rate"]
    )

    queue_depth = (
        row["queue_depth"]
    )

    snr = (
        row["signal_noise_ratio"]
    )

    status = str(
        row["system_status"]
    ).strip().upper()

    # ========================================================
    # High Temperature
    # ========================================================

    if (
        temperature is not None
        and temperature
        > HIGH_TEMPERATURE_MK
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "HIGH_TEMPERATURE",

                severity=
                    "HIGH",

                description=(
                    f"{device_id} temperature "
                    f"reached "
                    f"{temperature:.2f} mK, "
                    f"above threshold "
                    f"{HIGH_TEMPERATURE_MK:.2f} mK."
                ),
            )
        )

    # ========================================================
    # Hardware Error Spike
    # ========================================================

    if (
        hardware_error_rate is not None
        and hardware_error_rate
        > HIGH_HARDWARE_ERROR_RATE
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "HARDWARE_ERROR_SPIKE",

                severity=
                    "CRITICAL",

                description=(
                    f"{device_id} hardware "
                    f"error rate reached "
                    f"{hardware_error_rate:.6f}, "
                    f"above threshold "
                    f"{HIGH_HARDWARE_ERROR_RATE:.6f}."
                ),
            )
        )

    # ========================================================
    # Queue Pressure
    # ========================================================

    if (
        queue_depth is not None
        and queue_depth
        > HIGH_QUEUE_DEPTH
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "QUEUE_PRESSURE",

                severity=
                    "MEDIUM",

                description=(
                    f"{device_id} queue depth "
                    f"reached {queue_depth}, "
                    f"above threshold "
                    f"{HIGH_QUEUE_DEPTH}."
                ),
            )
        )

    # ========================================================
    # Low Signal-to-Noise Ratio
    # ========================================================

    if (
        snr is not None
        and snr
        < LOW_SIGNAL_NOISE_RATIO
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "LOW_SNR",

                severity=
                    "HIGH",

                description=(
                    f"{device_id} signal-to-noise "
                    f"ratio dropped to "
                    f"{snr:.2f}, "
                    f"below threshold "
                    f"{LOW_SIGNAL_NOISE_RATIO:.2f}."
                ),
            )
        )

    # ========================================================
    # Degraded Status
    # ========================================================

    if (
        status
        == "DEGRADED"
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "DEVICE_DEGRADED",

                severity=
                    "HIGH",

                description=(
                    f"{device_id} reported "
                    f"DEGRADED system status."
                ),
            )
        )

    # ========================================================
    # Critical Status
    # ========================================================

    if (
        status
        == "CRITICAL"
    ):

        incidents.append(
            create_incident(
                row=row,

                incident_type=
                    "DEVICE_CRITICAL",

                severity=
                    "CRITICAL",

                description=(
                    f"{device_id} reported "
                    f"CRITICAL system status."
                ),
            )
        )

    return incidents


# ============================================================
# Schema Migration / Verification
# ============================================================

def ensure_incident_schema(
    cursor,
):
    """
    Make the detector compatible with the existing
    qLore incidents table.

    We preserve:

        incident_id BIGSERIAL PRIMARY KEY

    and add:

        event_key
        event_timestamp
    """

    cursor.execute(
        """
        ALTER TABLE incidents

        ADD COLUMN IF NOT EXISTS
            event_key VARCHAR(64);
        """
    )

    cursor.execute(
        """
        ALTER TABLE incidents

        ADD COLUMN IF NOT EXISTS
            event_timestamp TIMESTAMP;
        """
    )

    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS

            idx_incidents_event_key

        ON incidents (
            event_key
        );
        """
    )


# ============================================================
# Write Incidents
# ============================================================

def write_incidents(
    cursor,
    incidents,
):

    inserted = 0

    existing = 0

    sql = """
        INSERT INTO incidents (

            event_key,

            device_id,

            incident_type,

            severity,

            description,

            event_timestamp,

            detected_at,

            status
        )

        VALUES (

            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )

        ON CONFLICT (
            event_key
        )

        DO NOTHING;
    """

    for incident in incidents:

        cursor.execute(
            sql,
            (

                incident[
                    "event_key"
                ],

                incident[
                    "device_id"
                ],

                incident[
                    "incident_type"
                ],

                incident[
                    "severity"
                ],

                incident[
                    "description"
                ],

                incident[
                    "event_timestamp"
                ],

                datetime.now(
                    timezone.utc
                ).replace(
                    tzinfo=None
                ),

                "OPEN",
            ),
        )

        if (
            cursor.rowcount
            == 1
        ):

            inserted += 1

        else:

            existing += 1

    return (
        inserted,
        existing,
    )


# ============================================================
# Main
# ============================================================

def main():

    print(
        "\nqLore Telemetry Anomaly Detector"
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

    # ========================================================
    # Load Silver telemetry
    # ========================================================

    print(
        "\nConnecting to Iceberg..."
    )

    catalog = (
        get_catalog()
    )

    print(
        "Connected to Iceberg catalog."
    )

    print(
        "\nLoading silver.telemetry..."
    )

    table = (
        catalog.load_table(
            (
                "silver",
                "telemetry",
            )
        )
    )

    print(
        "Loaded silver.telemetry."
    )

    print(
        "\nReading telemetry records..."
    )

    dataframe = (
        table
        .scan()
        .to_arrow()
        .to_pandas()
    )

    print(
        f"Silver telemetry rows scanned: "
        f"{len(dataframe)}"
    )

    # ========================================================
    # Detect anomalies
    # ========================================================

    print(
        "\nRunning anomaly rules..."
    )

    incidents = []

    for (
        _,
        row,
    ) in dataframe.iterrows():

        detected = (
            detect_row_anomalies(
                row
            )
        )

        incidents.extend(
            detected
        )

    print(
        f"Anomalies detected: "
        f"{len(incidents)}"
    )

    # ========================================================
    # PostgreSQL
    # ========================================================

    print(
        "\nConnecting to PostgreSQL..."
    )

    conn = (
        get_postgres_connection()
    )

    cursor = (
        conn.cursor()
    )

    print(
        "Connected to PostgreSQL."
    )

    # ========================================================
    # Ensure schema
    # ========================================================

    print(
        "\nVerifying incidents schema..."
    )

    ensure_incident_schema(
        cursor
    )

    conn.commit()

    print(
        "Incident schema ready."
    )

    # ========================================================
    # Write anomalies
    # ========================================================

    inserted, existing = (
        write_incidents(
            cursor,
            incidents,
        )
    )

    conn.commit()

    print(
        f"\nNew incidents inserted: "
        f"{inserted}"
    )

    print(
        f"Existing incidents skipped: "
        f"{existing}"
    )

    # ========================================================
    # Verify incidents
    # ========================================================

    cursor.execute(
        """
        SELECT

            incident_id,

            event_key,

            device_id,

            incident_type,

            severity,

            event_timestamp,

            detected_at,

            status

        FROM incidents

        ORDER BY
            detected_at DESC,
            incident_id DESC;
        """
    )

    rows = (
        cursor.fetchall()
    )

    print(
        f"\nTotal incidents in PostgreSQL: "
        f"{len(rows)}"
    )

    if rows:

        print(
            "\nIncident sample:"
        )

        print(
            "-" * 100
        )

        for row in rows[:20]:

            print(
                f"ID={row[0]} | "
                f"{row[2]} | "
                f"{row[3]} | "
                f"{row[4]} | "
                f"event={row[5]} | "
                f"status={row[7]}"
            )

    else:

        print(
            "\nNo incidents currently exist."
        )

        print(
            "All current Silver telemetry "
            "is within configured thresholds."
        )

    # ========================================================
    # Incident summary
    # ========================================================

    cursor.execute(
        """
        SELECT

            severity,

            COUNT(*)

        FROM incidents

        GROUP BY severity

        ORDER BY COUNT(*) DESC;
        """
    )

    summary = (
        cursor.fetchall()
    )

    if summary:

        print(
            "\nIncident severity summary:"
        )

        print(
            "-" * 50
        )

        for (
            severity,
            count,
        ) in summary:

            print(
                f"{severity}: "
                f"{count}"
            )

    # ========================================================
    # Cleanup
    # ========================================================

    cursor.close()

    conn.close()

    print(
        "\nAnomaly detection complete."
    )

    print("=" * 70)


if __name__ == "__main__":

    main()