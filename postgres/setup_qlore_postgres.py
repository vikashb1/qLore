import psycopg2


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "qlore",
    "user": "qlore",
    "password": "qlore",
}


def main():

    print("\nqLore PostgreSQL Setup")
    print("=" * 70)

    # ========================================================
    # Connect to PostgreSQL
    # ========================================================

    print("\nConnecting to PostgreSQL...")

    conn = psycopg2.connect(
        **DB_CONFIG
    )

    conn.autocommit = True

    cursor = conn.cursor()

    print("Connected successfully.")

    # ========================================================
    # Devices Table
    # ========================================================

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS devices (
            device_id VARCHAR(50) PRIMARY KEY,

            device_name VARCHAR(100),

            location VARCHAR(100),

            qubit_count INTEGER,

            operational_status VARCHAR(30),

            created_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    print(
        "Created / verified: devices"
    )

    # ========================================================
    # Current Device Health Table
    # ========================================================

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS device_health_current (

            device_id VARCHAR(50) PRIMARY KEY,

            total_events BIGINT,

            avg_temperature_mk DOUBLE PRECISION,

            avg_cpu_usage_pct DOUBLE PRECISION,

            avg_memory_usage_pct DOUBLE PRECISION,

            avg_queue_depth DOUBLE PRECISION,

            avg_active_jobs DOUBLE PRECISION,

            avg_signal_noise_ratio DOUBLE PRECISION,

            avg_cryogenic_pressure_mbar DOUBLE PRECISION,

            avg_hardware_error_rate DOUBLE PRECISION,

            healthy_events BIGINT,

            degraded_events BIGINT,

            critical_events BIGINT,

            health_score DOUBLE PRECISION,

            latest_event_timestamp TIMESTAMP,

            updated_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    print(
        "Created / verified: "
        "device_health_current"
    )

    # ========================================================
    # Incidents Table
    # ========================================================

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS incidents (

            incident_id BIGSERIAL PRIMARY KEY,

            device_id VARCHAR(50),

            incident_type VARCHAR(100),

            severity VARCHAR(30),

            description TEXT,

            detected_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP,

            resolved_at TIMESTAMP,

            status VARCHAR(30)
                DEFAULT 'OPEN'
        );
        """
    )

    print(
        "Created / verified: incidents"
    )

    # ========================================================
    # Pipeline Runs Table
    # ========================================================

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (

            pipeline_run_id BIGSERIAL PRIMARY KEY,

            pipeline_name VARCHAR(100),

            run_status VARCHAR(30),

            bronze_rows BIGINT,

            silver_rows BIGINT,

            gold_rows BIGINT,

            started_at TIMESTAMP,

            completed_at TIMESTAMP,

            created_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    print(
        "Created / verified: pipeline_runs"
    )

    # ========================================================
    # Verify Tables
    # ========================================================

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
    )

    tables = cursor.fetchall()

    print(
        "\nTables currently available:"
    )

    for table in tables:

        print(
            f"  - {table[0]}"
        )

    # ========================================================
    # Cleanup
    # ========================================================

    cursor.close()

    conn.close()

    print(
        "\nqLore PostgreSQL setup complete."
    )

    print("=" * 70)


if __name__ == "__main__":
    main()