from typing import Optional

from qlore_agent.tools.device_health import (
    normalize_device_id,
)

from qlore_agent.tools.query_trino import (
    query_trino,
)


# ============================================================
# Open Incidents
# ============================================================

def get_open_incidents(
    device_id: Optional[str] = None,
) -> dict:
    """
    Return aggregated OPEN qLore incidents.

    If device_id is supplied, only that device's
    incidents are returned.
    """

    normalized_device_id = None

    where_clause = ""

    if device_id:

        normalized_device_id = (
            normalize_device_id(
                device_id
            )
        )

        where_clause = (
            "WHERE device_id = "
            f"'{normalized_device_id}'"
        )

    sql = f"""
        SELECT

            device_id,

            incident_type,

            severity,

            occurrence_count,

            incident_start,

            incident_end,

            first_detected_at,

            last_detected_at

        FROM
            postgresql.public.incident_type_summary

        {where_clause}

        ORDER BY

            CASE severity

                WHEN 'CRITICAL'
                    THEN 1

                WHEN 'HIGH'
                    THEN 2

                WHEN 'MEDIUM'
                    THEN 3

                WHEN 'LOW'
                    THEN 4

                ELSE 5

            END,

            device_id,

            incident_type
    """

    result = (
        query_trino(
            sql
        )
    )

    incidents = []

    for row in result[
        "rows"
    ]:

        incidents.append(
            dict(
                zip(
                    result[
                        "columns"
                    ],
                    row,
                )
            )
        )

    return {
        "device_id":
            normalized_device_id,

        "incident_count":
            len(incidents),

        "incidents":
            incidents,
    }


# ============================================================
# Device Incident Summary
# ============================================================

def get_device_incident_summary(
    device_id: str,
) -> dict:
    """
    Return summarized open-incident statistics
    for one qLore device.
    """

    device_id = (
        normalize_device_id(
            device_id
        )
    )

    sql = f"""
        SELECT

            device_id,

            raw_incident_count,

            incident_type_count,

            critical_count,

            high_count,

            medium_count,

            incident_start,

            incident_end

        FROM
            postgresql.public.device_incident_summary

        WHERE
            device_id =
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
                f"No open incidents "
                f"found for {device_id}."
            ),
        }

    summary = dict(
        zip(
            result[
                "columns"
            ],
            result[
                "rows"
            ][0],
        )
    )

    return {
        "found": True,
        "summary": summary,
    }


# ============================================================
# Fleet Critical Incidents
# ============================================================

def get_critical_incidents() -> dict:
    """
    Return all aggregated CRITICAL incidents
    across the qLore fleet.
    """

    sql = """
        SELECT

            device_id,

            incident_type,

            severity,

            occurrence_count,

            incident_start,

            incident_end

        FROM
            postgresql.public.incident_type_summary

        WHERE
            severity = 'CRITICAL'

        ORDER BY
            occurrence_count DESC,
            device_id
    """

    result = (
        query_trino(
            sql
        )
    )

    incidents = []

    for row in result[
        "rows"
    ]:

        incidents.append(
            dict(
                zip(
                    result[
                        "columns"
                    ],
                    row,
                )
            )
        )

    return {
        "incident_count":
            len(incidents),

        "incidents":
            incidents,
    }


# ============================================================
# Local Verification
# ============================================================

def main():

    print()
    print(
        "qLore — Incident Tools"
    )
    print("=" * 70)

    device_id = "DEV002"

    print(
        f"\nOpen incidents for "
        f"{device_id}"
    )

    result = (
        get_open_incidents(
            device_id
        )
    )

    print(
        f"\nAggregated incident types: "
        f"{result['incident_count']}"
    )

    print(
        "-" * 80
    )

    for incident in result[
        "incidents"
    ]:

        print(
            f"{incident['severity']:8} | "
            f"{incident['incident_type']:25} | "
            f"occurrences="
            f"{incident['occurrence_count']}"
        )

    print(
        "\nDevice incident summary:"
    )

    summary_result = (
        get_device_incident_summary(
            device_id
        )
    )

    if summary_result[
        "found"
    ]:

        for (
            key,
            value,
        ) in summary_result[
            "summary"
        ].items():

            print(
                f"{key}: {value}"
            )

    else:

        print(
            summary_result[
                "message"
            ]
        )

    print(
        "\nCritical fleet incidents:"
    )

    critical = (
        get_critical_incidents()
    )

    print(
        f"Critical incident categories: "
        f"{critical['incident_count']}"
    )

    print()
    print("=" * 70)
    print(
        "Incident tool verification complete."
    )


if __name__ == "__main__":
    main()