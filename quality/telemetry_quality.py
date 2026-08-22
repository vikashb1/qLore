from datetime import datetime, timedelta

import pandas as pd


REQUIRED_COLUMNS = [
    "device_id",
    "event_timestamp",
    "schema_version",
]

VALID_STATUS_VALUES = {
    "HEALTHY",
    "DEGRADED",
    "CRITICAL",
}


def check_required_columns(
    df: pd.DataFrame,
) -> list[str]:
    """
    Verify required columns exist.
    """

    errors = []

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        errors.append(
            f"Missing required columns: "
            f"{missing_columns}"
        )

    return errors


def check_not_null(
    df: pd.DataFrame,
) -> list[str]:
    """
    Verify required fields do not contain NULL values.
    """

    errors = []

    for column in REQUIRED_COLUMNS:

        if column not in df.columns:
            continue

        null_count = (
            df[column]
            .isna()
            .sum()
        )

        if null_count > 0:

            errors.append(
                f"{column} contains "
                f"{null_count} NULL values."
            )

    return errors


def check_ranges(
    df: pd.DataFrame,
) -> list[str]:
    """
    Validate telemetry values fall within
    reasonable operating ranges.
    """

    errors = []

    range_rules = {
        "temperature_mk": (
            5,
            40,
        ),

        "cpu_usage_pct": (
            0,
            100,
        ),

        "memory_usage_pct": (
            0,
            100,
        ),

        "queue_depth": (
            0,
            500,
        ),

        "active_jobs": (
            0,
            500,
        ),

        "signal_noise_ratio": (
            0,
            100,
        ),

        "cryogenic_pressure_mbar": (
            0,
            5,
        ),

        "hardware_error_rate": (
            0,
            1,
        ),
    }

    for column, (
        minimum,
        maximum,
    ) in range_rules.items():

        if column not in df.columns:
            continue

        invalid_mask = (
            df[column].notna()
            & (
                (df[column] < minimum)
                | (df[column] > maximum)
            )
        )

        invalid_count = (
            invalid_mask.sum()
        )

        if invalid_count > 0:

            errors.append(
                f"{column} has "
                f"{invalid_count} values "
                f"outside [{minimum}, {maximum}]."
            )

    return errors


def check_status_values(
    df: pd.DataFrame,
) -> list[str]:
    """
    Validate system_status values.
    """

    errors = []

    if "system_status" not in df.columns:
        return errors

    invalid_statuses = set(
        df[
            df["system_status"].notna()
        ]["system_status"]
    ) - VALID_STATUS_VALUES

    if invalid_statuses:

        errors.append(
            f"Invalid system_status values: "
            f"{sorted(invalid_statuses)}"
        )

    return errors


def check_uniqueness(
    df: pd.DataFrame,
) -> list[str]:
    """
    Kafka partition + offset should uniquely
    identify each ingested Kafka record.
    """

    errors = []

    required = {
        "kafka_partition",
        "kafka_offset",
    }

    if not required.issubset(
        df.columns
    ):
        return errors

    duplicate_count = (
        df.duplicated(
            subset=[
                "kafka_partition",
                "kafka_offset",
            ]
        )
        .sum()
    )

    if duplicate_count > 0:

        errors.append(
            f"Found {duplicate_count} duplicate "
            f"Kafka partition/offset pairs."
        )

    return errors


def check_freshness(
    df: pd.DataFrame,
    max_age_hours: int = 24,
) -> list[str]:
    """
    Verify the newest telemetry event is recent.

    We use 24 hours for the local development
    environment so the check remains practical.
    """

    errors = []

    if (
        "event_timestamp"
        not in df.columns
    ):
        return errors

    if df.empty:
        errors.append(
            "Telemetry dataset is empty."
        )
        return errors

    timestamps = pd.to_datetime(
        df["event_timestamp"],
        errors="coerce",
    )

    if timestamps.isna().all():

        errors.append(
            "No valid event timestamps found."
        )

        return errors

    latest_event = (
        timestamps.max()
    )

    now = datetime.utcnow()

    maximum_age = timedelta(
        hours=max_age_hours
    )

    age = (
        now
        - latest_event.to_pydatetime()
    )

    if age > maximum_age:

        errors.append(
            f"Telemetry is stale. "
            f"Latest event age: {age}."
        )

    return errors


def run_telemetry_quality_checks(
    df: pd.DataFrame,
) -> dict:
    """
    Run all qLore Bronze telemetry
    data-quality checks.
    """

    all_errors = []

    checks = [
        (
            "required_columns",
            check_required_columns,
        ),
        (
            "not_null",
            check_not_null,
        ),
        (
            "ranges",
            check_ranges,
        ),
        (
            "status_values",
            check_status_values,
        ),
        (
            "uniqueness",
            check_uniqueness,
        ),
        (
            "freshness",
            check_freshness,
        ),
    ]

    results = {}

    for (
        check_name,
        check_function,
    ) in checks:

        errors = check_function(
            df
        )

        passed = (
            len(errors) == 0
        )

        results[
            check_name
        ] = {
            "passed": passed,
            "errors": errors,
        }

        all_errors.extend(
            errors
        )

    results["overall"] = {
        "passed":
            len(all_errors) == 0,

        "error_count":
            len(all_errors),

        "errors":
            all_errors,
    }

    return results