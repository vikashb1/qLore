import psycopg2


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "qlore",
    "user": "qlore",
    "password": "qlore",
}


def main():

    print("\nqLore Incident Aggregation Setup")
    print("=" * 70)

    print("\nConnecting to PostgreSQL...")

    conn = psycopg2.connect(
        **DB_CONFIG
    )

    conn.autocommit = True

    cursor = conn.cursor()

    print("Connected.")

    # ========================================================
    # Incident Type Summary
    # ========================================================

    cursor.execute(
        """
        CREATE OR REPLACE VIEW incident_type_summary AS

        SELECT

            device_id,

            incident_type,

            severity,

            COUNT(*) AS occurrence_count,

            MIN(event_timestamp)
                AS incident_start,

            MAX(event_timestamp)
                AS incident_end,

            MIN(detected_at)
                AS first_detected_at,

            MAX(detected_at)
                AS last_detected_at

        FROM incidents

        WHERE status = 'OPEN'

        GROUP BY
            device_id,
            incident_type,
            severity;
        """
    )

    print(
        "Created / updated: "
        "incident_type_summary"
    )

    # ========================================================
    # Device-Level Incident Summary
    # ========================================================

    cursor.execute(
        """
        CREATE OR REPLACE VIEW device_incident_summary AS

        SELECT

            device_id,

            COUNT(*) AS raw_incident_count,

            COUNT(
                DISTINCT incident_type
            ) AS incident_type_count,

            SUM(
                CASE
                    WHEN severity = 'CRITICAL'
                    THEN 1
                    ELSE 0
                END
            ) AS critical_count,

            SUM(
                CASE
                    WHEN severity = 'HIGH'
                    THEN 1
                    ELSE 0
                END
            ) AS high_count,

            SUM(
                CASE
                    WHEN severity = 'MEDIUM'
                    THEN 1
                    ELSE 0
                END
            ) AS medium_count,

            MIN(event_timestamp)
                AS incident_start,

            MAX(event_timestamp)
                AS incident_end

        FROM incidents

        WHERE status = 'OPEN'

        GROUP BY
            device_id;
        """
    )

    print(
        "Created / updated: "
        "device_incident_summary"
    )

    # ========================================================
    # Verification
    # ========================================================

    cursor.execute(
        """
        SELECT
            device_id,
            incident_type,
            severity,
            occurrence_count,
            incident_start,
            incident_end
        FROM incident_type_summary
        ORDER BY
            device_id,
            severity,
            incident_type;
        """
    )

    rows = cursor.fetchall()

    print(
        "\nAggregated incident types:"
    )

    print("-" * 100)

    for row in rows:

        print(
            f"{row[0]} | "
            f"{row[1]} | "
            f"{row[2]} | "
            f"occurrences={row[3]} | "
            f"start={row[4]} | "
            f"end={row[5]}"
        )

    cursor.close()

    conn.close()

    print(
        "\nIncident aggregation setup complete."
    )

    print("=" * 70)


if __name__ == "__main__":
    main()