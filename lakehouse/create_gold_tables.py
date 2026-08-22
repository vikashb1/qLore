from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    NestedField,
)


def get_catalog():

    return load_catalog(
        "qlore",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://warehouse",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def device_health_schema():

    return Schema(

        NestedField(
            1,
            "device_id",
            StringType(),
            required=True,
        ),

        NestedField(
            2,
            "total_events",
            LongType(),
            required=True,
        ),

        NestedField(
            3,
            "avg_temperature_mk",
            DoubleType(),
            required=False,
        ),

        NestedField(
            4,
            "avg_cpu_usage_pct",
            DoubleType(),
            required=False,
        ),

        NestedField(
            5,
            "avg_memory_usage_pct",
            DoubleType(),
            required=False,
        ),

        NestedField(
            6,
            "avg_queue_depth",
            DoubleType(),
            required=False,
        ),

        NestedField(
            7,
            "avg_active_jobs",
            DoubleType(),
            required=False,
        ),

        NestedField(
            8,
            "avg_signal_noise_ratio",
            DoubleType(),
            required=False,
        ),

        NestedField(
            9,
            "avg_cryogenic_pressure_mbar",
            DoubleType(),
            required=False,
        ),

        NestedField(
            10,
            "avg_hardware_error_rate",
            DoubleType(),
            required=False,
        ),

        NestedField(
            11,
            "healthy_events",
            LongType(),
            required=True,
        ),

        NestedField(
            12,
            "degraded_events",
            LongType(),
            required=True,
        ),

        NestedField(
            13,
            "critical_events",
            LongType(),
            required=True,
        ),

        NestedField(
            14,
            "health_score",
            DoubleType(),
            required=True,
        ),

        NestedField(
            15,
            "latest_event_timestamp",
            TimestampType(),
            required=True,
        ),
    )


def main():

    print("\nqLore Gold Table Setup")
    print("=" * 70)

    catalog = get_catalog()

    identifier = (
        "gold",
        "device_health",
    )

    existing_tables = catalog.list_tables(
        "gold"
    )

    if identifier in existing_tables:

        print(
            "gold.device_health already exists."
        )

        table = catalog.load_table(
            identifier
        )

    else:

        table = catalog.create_table(
            identifier=identifier,
            schema=device_health_schema(),
        )

        print(
            "Created Iceberg table: "
            "gold.device_health"
        )

    print("\nSchema:")
    print(table.schema())

    print("\nLocation:")
    print(table.location())


if __name__ == "__main__":
    main()