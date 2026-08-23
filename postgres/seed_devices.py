import psycopg2


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "qlore",
    "user": "qlore",
    "password": "qlore",
}


DEVICES = [
    (
        "DEV001",
        "Quantum Processor Alpha",
        "Phoenix Lab A",
        127,
        "ACTIVE",
    ),
    (
        "DEV002",
        "Quantum Processor Beta",
        "Phoenix Lab A",
        127,
        "ACTIVE",
    ),
    (
        "DEV003",
        "Quantum Processor Gamma",
        "Phoenix Lab B",
        433,
        "ACTIVE",
    ),
    (
        "DEV004",
        "Quantum Processor Delta",
        "Phoenix Lab B",
        433,
        "ACTIVE",
    ),
    (
        "DEV005",
        "Quantum Processor Epsilon",
        "Phoenix Lab C",
        1121,
        "ACTIVE",
    ),
]


def main():

    print("\nqLore PostgreSQL Device Seeder")
    print("=" * 70)

    print("\nConnecting to PostgreSQL...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Connected.")

    for device in DEVICES:

        cursor.execute(
            """
            INSERT INTO devices (
                device_id,
                device_name,
                location,
                qubit_count,
                operational_status
            )
            VALUES (%s, %s, %s, %s, %s)

            ON CONFLICT (device_id)
            DO UPDATE SET
                device_name = EXCLUDED.device_name,
                location = EXCLUDED.location,
                qubit_count = EXCLUDED.qubit_count,
                operational_status = EXCLUDED.operational_status;
            """,
            device,
        )

    conn.commit()

    cursor.execute(
        """
        SELECT
            device_id,
            device_name,
            location,
            qubit_count,
            operational_status
        FROM devices
        ORDER BY device_id;
        """
    )

    rows = cursor.fetchall()

    print(f"\nDevices loaded: {len(rows)}")
    print("-" * 70)

    for row in rows:
        print(
            f"{row[0]} | "
            f"{row[1]} | "
            f"{row[2]} | "
            f"{row[3]} qubits | "
            f"{row[4]}"
        )

    cursor.close()
    conn.close()

    print("\nDevice seeding complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()