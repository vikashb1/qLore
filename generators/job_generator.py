from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


RANDOM_SEED = 43
TOTAL_JOBS = 100_000


def load_devices() -> pd.DataFrame:
    path = Path("data/generated/devices.csv")

    if not path.exists():
        raise FileNotFoundError(
            "devices.csv not found. Run device_generator.py first."
        )

    return pd.read_csv(path)


def load_calibrations() -> pd.DataFrame:
    path = Path("data/generated/calibrations.csv")

    if not path.exists():
        raise FileNotFoundError(
            "calibrations.csv not found. "
            "Run calibration_generator.py first."
        )

    df = pd.read_csv(
        path,
        parse_dates=["calibration_timestamp"],
    )

    return df


def get_daily_device_health(
    calibrations_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate qubit-level calibration data into
    daily device-level health metrics.
    """

    calibrations_df["date"] = (
        calibrations_df["calibration_timestamp"]
        .dt.date
    )

    daily_health = (
        calibrations_df
        .groupby(["device_id", "date"])
        .agg(
            avg_gate_error=("gate_error", "mean"),
            avg_readout_error=("readout_error", "mean"),
            avg_t1_us=("t1_us", "mean"),
            avg_t2_us=("t2_us", "mean"),
            failed_calibrations=(
                "calibration_status",
                lambda x: (x == "FAIL").sum(),
            ),
        )
        .reset_index()
    )

    return daily_health


def calculate_failure_probability(
    device_id: str,
    job_date,
    daily_health_df: pd.DataFrame,
) -> float:
    """
    Base failure rate is ~3%.

    Failure probability increases when device
    calibration quality deteriorates.
    """

    base_failure_probability = 0.03

    health = daily_health_df[
        (daily_health_df["device_id"] == device_id)
        & (daily_health_df["date"] == job_date)
    ]

    if health.empty:
        return base_failure_probability

    row = health.iloc[0]

    failure_probability = base_failure_probability

    # Device-level degradation contribution
    failure_probability += (
        row["avg_gate_error"] * 3
    )

    failure_probability += (
        row["avg_readout_error"] * 0.4
    )

    failure_probability += (
        row["failed_calibrations"] * 0.005
    )

    # Our controlled qLore demo anomaly:
    # DEV002 deteriorates near the end of the period.
    if device_id == "DEV002":
        recent_cutoff = (
            daily_health_df["date"].max()
            - timedelta(days=9)
        )

        if job_date >= recent_cutoff:
            days_into_drift = (
                job_date - recent_cutoff
            ).days

            failure_probability += (
                days_into_drift * 0.018
            )

    return min(
        max(failure_probability, 0.01),
        0.35,
    )


def generate_jobs(
    devices_df: pd.DataFrame,
    calibrations_df: pd.DataFrame,
    total_jobs: int = TOTAL_JOBS,
) -> pd.DataFrame:

    rng = np.random.default_rng(RANDOM_SEED)

    daily_health = get_daily_device_health(
        calibrations_df
    )

    start_date = (
        calibrations_df[
            "calibration_timestamp"
        ].min()
    )

    end_date = (
        calibrations_df[
            "calibration_timestamp"
        ].max()
        + timedelta(hours=23, minutes=59)
    )

    total_seconds = int(
        (end_date - start_date).total_seconds()
    )

    device_ids = devices_df["device_id"].tolist()

    records = []

    for job_number in range(
        1,
        total_jobs + 1,
    ):

        device_id = rng.choice(device_ids)

        submitted_at = (
            start_date
            + timedelta(
                seconds=int(
                    rng.integers(
                        0,
                        total_seconds,
                    )
                )
            )
        )

        job_date = submitted_at.date()

        queue_time_ms = int(
            rng.lognormal(
                mean=7.5,
                sigma=0.7,
            )
        )

        queued_at = submitted_at + timedelta(
            milliseconds=int(
                rng.integers(20, 500)
            )
        )

        started_at = queued_at + timedelta(
            milliseconds=queue_time_ms
        )

        execution_time_ms = int(
            rng.lognormal(
                mean=6.5,
                sigma=0.6,
            )
        )

        shots = int(
            rng.choice(
                [100, 500, 1000, 2000, 4000],
                p=[0.05, 0.15, 0.40, 0.25, 0.15],
            )
        )

        circuit_depth = int(
            rng.integers(5, 250)
        )

        failure_probability = (
            calculate_failure_probability(
                device_id,
                job_date,
                daily_health,
            )
        )

        failed = (
            rng.random()
            < failure_probability
        )

        if failed:

            failure_type = rng.choice(
                [
                    "CALIBRATION_ERROR",
                    "EXECUTION_ERROR",
                    "DEVICE_ERROR",
                    "TIMEOUT",
                ],
                p=[
                    0.50,
                    0.20,
                    0.20,
                    0.10,
                ],
            )

            status = "FAILED"
            error_code = failure_type

        else:

            status = "SUCCESS"
            error_code = None

        completed_at = (
            started_at
            + timedelta(
                milliseconds=execution_time_ms
            )
        )

        records.append(
            {
                "job_id": f"JOB{job_number:08d}",
                "user_id": (
                    f"USR{rng.integers(1, 5001):05d}"
                ),
                "device_id": device_id,
                "submitted_at": submitted_at,
                "queued_at": queued_at,
                "started_at": started_at,
                "completed_at": completed_at,
                "queue_time_ms": queue_time_ms,
                "execution_time_ms": execution_time_ms,
                "shots": shots,
                "circuit_depth": circuit_depth,
                "status": status,
                "error_code": error_code,
            }
        )

    return pd.DataFrame(records)


def save_jobs(
    jobs_df: pd.DataFrame,
) -> Path:

    output_dir = Path("data/generated")

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir / "jobs.csv"
    )

    jobs_df.to_csv(
        output_path,
        index=False,
    )

    return output_path


def print_summary(
    jobs_df: pd.DataFrame,
):

    print("\nqLore Job Execution Dataset")
    print("=" * 80)

    print(
        f"Total jobs: "
        f"{len(jobs_df):,}"
    )

    print("\nStatus distribution:")

    print(
        jobs_df[
            "status"
        ].value_counts()
    )

    print("\nDevice-level failure rates:")

    device_failure_rates = (
        jobs_df
        .assign(
            failed=(
                jobs_df["status"]
                == "FAILED"
            )
        )
        .groupby("device_id")["failed"]
        .mean()
        .mul(100)
        .round(2)
    )

    print(device_failure_rates)


if __name__ == "__main__":

    devices_df = load_devices()

    calibrations_df = load_calibrations()

    jobs_df = generate_jobs(
        devices_df,
        calibrations_df,
    )

    output_path = save_jobs(
        jobs_df
    )

    print_summary(
        jobs_df
    )

    print(
        f"\nSaved to: {output_path}"
    )