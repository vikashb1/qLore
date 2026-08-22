from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
    NestedField,
)


# ============================================================
# qLore Iceberg Configuration
# ============================================================

CATALOG_URI = "http://localhost:8181"
WAREHOUSE = "s3://warehouse"


def get_catalog():
    """
    Connect to the qLore Iceberg REST catalog.

    The Python script runs on Windows, so it accesses
    MinIO through localhost:9000.

    The Iceberg REST container itself accesses MinIO
    internally through minio:9000.
    """

    return load_catalog(
        "qlore",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


# ============================================================
# Bronze Telemetry Schema
# ============================================================

def telemetry_schema():
    """
    Define the schema for bronze.telemetry.

    The table supports both telemetry schema V1 and V2.

    V2 fields:
        - cryogenic_pressure_mbar
        - hardware_error_rate

    These are optional so older V1 records can still
    be stored in the table.
    """

    return Schema(

        NestedField(
            field_id=1,
            name="device_id",
            field_type=StringType(),
            required=True,
        ),

        NestedField(
            field_id=2,
            name="event_timestamp",
            field_type=TimestampType(),
            required=True,
        ),

        NestedField(
            field_id=3,
            name="temperature_mk",
            field_type=DoubleType(),
            required=False,
        ),

        NestedField(
            field_id=4,
            name="cpu_usage_pct",
            field_type=DoubleType(),
            required=False,
        ),

        NestedField(
            field_id=5,
            name="memory_usage_pct",
            field_type=DoubleType(),
            required=False,
        ),

        NestedField(
            field_id=6,
            name="queue_depth",
            field_type=IntegerType(),
            required=False,
        ),

        NestedField(
            field_id=7,
            name="active_jobs",
            field_type=IntegerType(),
            required=False,
        ),

        NestedField(
            field_id=8,
            name="signal_noise_ratio",
            field_type=DoubleType(),
            required=False,
        ),

        # Added in telemetry schema V2
        NestedField(
            field_id=9,
            name="cryogenic_pressure_mbar",
            field_type=DoubleType(),
            required=False,
        ),

        # Added in telemetry schema V2
        NestedField(
            field_id=10,
            name="hardware_error_rate",
            field_type=DoubleType(),
            required=False,
        ),

        NestedField(
            field_id=11,
            name="system_status",
            field_type=StringType(),
            required=False,
        ),

        NestedField(
            field_id=12,
            name="schema_version",
            field_type=StringType(),
            required=True,
        ),
    )


# ============================================================
# Create Bronze Telemetry Table
# ============================================================

def create_bronze_telemetry_table(catalog):
    """
    Create bronze.telemetry if it does not already exist.
    """

    table_identifier = (
        "bronze",
        "telemetry",
    )

    print(
        "\nChecking for bronze.telemetry..."
    )

    existing_tables = catalog.list_tables(
        "bronze"
    )

    if table_identifier in existing_tables:

        print(
            "bronze.telemetry already exists."
        )

        table = catalog.load_table(
            table_identifier
        )

    else:

        print(
            "Creating bronze.telemetry..."
        )

        table = catalog.create_table(
            identifier=table_identifier,
            schema=telemetry_schema(),
        )

        print(
            "Created Iceberg table: "
            "bronze.telemetry"
        )

    return table


# ============================================================
# Main
# ============================================================

def main():

    print(
        "\nqLore Bronze Iceberg Table Setup"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Connect to Iceberg REST Catalog
    # --------------------------------------------------------

    print(
        "\nConnecting to Iceberg REST Catalog..."
    )

    catalog = get_catalog()

    print(
        "Connected to Iceberg REST Catalog."
    )

    # --------------------------------------------------------
    # Check Bronze namespace
    # --------------------------------------------------------

    print(
        "\nChecking Iceberg namespaces..."
    )

    namespaces = catalog.list_namespaces()

    namespace_names = {
        ".".join(namespace)
        for namespace in namespaces
    }

    if "bronze" not in namespace_names:

        print(
            "Bronze namespace does not exist."
        )

        print(
            "Creating bronze namespace..."
        )

        catalog.create_namespace(
            "bronze"
        )

        print(
            "Created namespace: bronze"
        )

    else:

        print(
            "Namespace exists: bronze"
        )

    # --------------------------------------------------------
    # Create Bronze Telemetry Table
    # --------------------------------------------------------

    table = create_bronze_telemetry_table(
        catalog
    )

    # --------------------------------------------------------
    # Display table information
    # --------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "BRONZE TELEMETRY TABLE"
    )

    print("=" * 70)

    print(
        "\nTable name:"
    )

    print(
        "bronze.telemetry"
    )

    print(
        "\nTable location:"
    )

    print(
        table.location()
    )

    print(
        "\nTable schema:"
    )

    print(
        table.schema()
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "qLore Bronze Iceberg table setup complete."
    )

    print("=" * 70)


if __name__ == "__main__":
    main()