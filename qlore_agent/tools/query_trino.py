import os

import requests


# ============================================================
# Configuration
# ============================================================

TRINO_URL = os.getenv(
    "QLORE_TRINO_URL",
    "http://127.0.0.1:8081/v1/statement",
)

DEFAULT_CATALOG = "iceberg"
DEFAULT_SCHEMA = "gold"


HEADERS = {
    "X-Trino-User": "qlore-agent",
    "X-Trino-Catalog": DEFAULT_CATALOG,
    "X-Trino-Schema": DEFAULT_SCHEMA,
}


# ============================================================
# Exceptions
# ============================================================

class TrinoQueryError(Exception):
    """
    Raised when Trino returns a query error.
    """


# ============================================================
# Read-only SQL validation
# ============================================================

def validate_read_only_sql(
    sql: str,
) -> str:
    """
    Validate that the SQL is read-only.

    qLore's agent is intentionally prevented from
    modifying Iceberg or PostgreSQL through Trino.
    """

    if not sql:
        raise ValueError(
            "SQL query cannot be empty."
        )

    cleaned_sql = sql.strip()

    if not cleaned_sql:
        raise ValueError(
            "SQL query cannot be empty."
        )

    first_word = (
        cleaned_sql
        .split()[0]
        .upper()
    )

    allowed_commands = {
        "SELECT",
        "SHOW",
        "DESCRIBE",
        "WITH",
        "EXPLAIN",
    }

    if first_word not in allowed_commands:

        raise ValueError(
            "qLore only permits read-only "
            "Trino queries."
        )

    blocked_keywords = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "CREATE",
        "TRUNCATE",
        "MERGE",
        "CALL",
        "GRANT",
        "REVOKE",
    }

    upper_sql = (
        cleaned_sql.upper()
    )

    for keyword in blocked_keywords:

        if f" {keyword} " in (
            f" {upper_sql} "
        ):

            raise ValueError(
                f"Blocked SQL operation: "
                f"{keyword}"
            )

    return cleaned_sql


# ============================================================
# Query Trino
# ============================================================

def query_trino(
    sql: str,
) -> dict:
    """
    Execute a read-only SQL query against qLore Trino.

    Trino provides federated access to:

        iceberg.bronze.*
        iceberg.silver.*
        iceberg.gold.*
        postgresql.public.*

    Returns:

        {
            "columns": [...],
            "rows": [...],
            "row_count": 0
        }
    """

    cleaned_sql = (
        validate_read_only_sql(
            sql
        )
    )

    response = requests.post(
        TRINO_URL,
        headers=HEADERS,
        data=cleaned_sql.encode(
            "utf-8"
        ),
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    columns = []

    rows = []

    # Trino may return several pages.
    while True:

        if "error" in payload:

            error = payload[
                "error"
            ]

            message = error.get(
                "message",
                "Unknown Trino error",
            )

            raise TrinoQueryError(
                message
            )

        if (
            payload.get("columns")
            and not columns
        ):

            columns = [
                column["name"]
                for column
                in payload["columns"]
            ]

        if payload.get("data"):

            rows.extend(
                payload["data"]
            )

        next_uri = payload.get(
            "nextUri"
        )

        if not next_uri:
            break

        response = requests.get(
            next_uri,
            headers=HEADERS,
            timeout=30,
        )

        response.raise_for_status()

        payload = (
            response.json()
        )

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }


# ============================================================
# Pretty Printer
# ============================================================

def print_result(
    result: dict,
) -> None:

    columns = result[
        "columns"
    ]

    rows = result[
        "rows"
    ]

    if not rows:

        print(
            "No rows returned."
        )

        return

    print()

    print(
        " | ".join(
            str(column)
            for column in columns
        )
    )

    print(
        "-" * 100
    )

    for row in rows:

        print(
            " | ".join(
                str(value)
                for value in row
            )
        )

    print()

    print(
        f"Rows returned: "
        f"{result['row_count']}"
    )


# ============================================================
# Local Verification
# ============================================================

def main():

    print()
    print(
        "qLore — Trino Query Tool"
    )
    print("=" * 70)

    print(
        "\nTEST 1 — Gold health"
    )

    result = query_trino(
        """
        SELECT
            device_id,
            total_events,
            ROUND(
                avg_temperature_mk,
                2
            ) AS avg_temperature_mk,
            ROUND(
                health_score,
                2
            ) AS health_score

        FROM iceberg.gold.device_health

        ORDER BY device_id
        """
    )

    print_result(
        result
    )

    print(
        "\nTEST 2 — Incidents"
    )

    result = query_trino(
        """
        SELECT
            device_id,
            incident_type,
            severity,
            occurrence_count

        FROM postgresql.public.incident_type_summary

        ORDER BY
            device_id,
            incident_type
        """
    )

    print_result(
        result
    )

    print()
    print("=" * 70)
    print(
        "Trino query tool verification complete."
    )


if __name__ == "__main__":
    main()