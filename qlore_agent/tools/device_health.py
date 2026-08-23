import re

from qlore_agent.tools.query_trino import (
    query_trino,
)


# ============================================================
# Device Validation
# ============================================================

def normalize_device_id(
    device_id: str,
) -> str:

    if not device_id:

        raise ValueError(
            "device_id is required."
        )

    normalized = (
        device_id
        .strip()
        .upper()
    )

    if not re.fullmatch(
        r"DEV\d{3}",
        normalized,
    ):

        raise ValueError(
            "Invalid device ID. "
            "Expected format such as DEV002."
        )

    return normalized


# ============================================================
# Device Health
# ============================================================

def get_device_health(
    device_id: str,
) -> dict:
    """
    Return current operational and health information
    for one qLore device.

    Combines PostgreSQL device metadata with
    Iceberg Gold health metrics through Trino.
    """

    device_id = (
        normalize_device_id(
            device_id
        )
    )

    sql = f"""
        SELECT

            d.device_id,

            d.device_name,

            d.location,

            d.qubit_count,

            d.operational_status,

            g.total_events,

            ROUND(
                g.avg_temperature_mk,
                2
            ) AS avg_temperature_mk,

            ROUND(
                g.avg_cpu_usage_pct,
                2
            ) AS avg_cpu_usage_pct,

            ROUND(
                g.avg_memory_usage_pct,
                2
            ) AS avg_memory_usage_pct,

            ROUND(
                g.avg_queue_depth,
                2
            ) AS avg_queue_depth,

            ROUND(
                g.avg_active_jobs,
                2
            ) AS avg_active_jobs,

            ROUND(
                g.avg_signal_noise_ratio,
                2
            ) AS avg_signal_noise_ratio,

            ROUND(
                g.avg_cryogenic_pressure_mbar,
                4
            ) AS avg_cryogenic_pressure_mbar,

            ROUND(
                g.avg_hardware_error_rate,
                6
            ) AS avg_hardware_error_rate,

            ROUND(
                g.health_score,
                2
            ) AS health_score,

            g.latest_event_timestamp

        FROM postgresql.public.devices d

        LEFT JOIN
            iceberg.gold.device_health g

            ON d.device_id =
               g.device_id

        WHERE
            d.device_id =
            '{device_id}'
    """

    result = (
        query_trino(
            sql
        )
    )

    if result[
        "row_count"
    ] == 0:

        return {
            "found": False,
            "device_id": device_id,
            "message": (
                f"Device {device_id} "
                f"was not found."
            ),
        }

    device = dict(
        zip(
            result["columns"],
            result["rows"][0],
        )
    )

    return {
        "found": True,
        "device": device,
    }


# ============================================================
# Local Verification
# ============================================================

def main():

    print()
    print(
        "qLore — Device Health Tool"
    )
    print("=" * 70)

    device_id = "DEV002"

    print(
        f"\nLooking up "
        f"{device_id}..."
    )

    result = (
        get_device_health(
            device_id
        )
    )

    if not result[
        "found"
    ]:

        print(
            result["message"]
        )

        return

    print()

    for (
        key,
        value,
    ) in result[
        "device"
    ].items():

        print(
            f"{key}: {value}"
        )

    print()
    print("=" * 70)
    print(
        "Device health tool verification complete."
    )


if __name__ == "__main__":
    main()