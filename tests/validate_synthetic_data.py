from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/generated")


def load_data():

    calibrations = pd.read_csv(
        DATA_DIR / "calibrations.csv",
        parse_dates=["calibration_timestamp"],
    )

    jobs = pd.read_csv(
        DATA_DIR / "jobs.csv",
        parse_dates=["submitted_at"],
    )

    telemetry = pd.read_csv(
        DATA_DIR / "telemetry.csv",
        parse_dates=["event_timestamp"],
    )

    return calibrations, jobs, telemetry


def validate_calibration_incident(
    calibrations: pd.DataFrame,
):

    print("\n1. CALIBRATION INCIDENT")
    print("=" * 70)

    target = calibrations[
        (calibrations["device_id"] == "DEV002")
        & (calibrations["qubit_id"] == "Q017")
    ].sort_values(
        "calibration_timestamp"
    )

    recent = target.tail(10)

    print(
        recent[
            [
                "calibration_timestamp",
                "t1_us",
                "t2_us",
                "gate_error",
                "readout_error",
                "calibration_status",
            ]
        ].to_string(index=False)
    )

    failures = (
        recent["calibration_status"]
        == "FAIL"
    ).sum()

    assert failures > 0, (
        "DEV002/Q017 never enters FAIL state."
    )

    print(
        f"\nPASS: DEV002/Q017 contains "
        f"{failures} failed calibrations."
    )


def validate_telemetry_incident(
    telemetry: pd.DataFrame,
):

    print("\n\n2. TELEMETRY INCIDENT")
    print("=" * 70)

    dev002 = telemetry[
        telemetry["device_id"]
        == "DEV002"
    ]

    status_counts = (
        dev002["system_status"]
        .value_counts()
    )

    print(status_counts)

    degraded = (
        dev002["system_status"]
        == "DEGRADED"
    ).sum()

    critical = (
        dev002["system_status"]
        == "CRITICAL"
    ).sum()

    assert degraded > 0, (
        "DEV002 has no DEGRADED telemetry."
    )

    assert critical > 0, (
        "DEV002 has no CRITICAL telemetry."
    )

    print(
        f"\nPASS: DEV002 contains "
        f"{degraded} DEGRADED and "
        f"{critical} CRITICAL events."
    )


def validate_job_failure_incident(
    jobs: pd.DataFrame,
):

    print("\n\n3. JOB FAILURE INCIDENT")
    print("=" * 70)

    jobs = jobs.copy()

    jobs["failed"] = (
        jobs["status"]
        == "FAILED"
    )

    failure_rates = (
        jobs
        .groupby("device_id")["failed"]
        .mean()
        .mul(100)
        .round(2)
        .sort_values(
            ascending=False
        )
    )

    print(failure_rates)

    dev002_rate = failure_rates[
        "DEV002"
    ]

    healthy_rates = failure_rates.drop(
        "DEV002"
    )

    healthy_average = (
        healthy_rates.mean()
    )

    assert dev002_rate > healthy_average, (
        "DEV002 failure rate is not "
        "higher than healthy-device average."
    )

    print(
        f"\nDEV002 failure rate: "
        f"{dev002_rate:.2f}%"
    )

    print(
        f"Other-device average: "
        f"{healthy_average:.2f}%"
    )

    print(
        "\nPASS: DEV002 has elevated "
        "job failures."
    )


def validate_referential_integrity(
    calibrations: pd.DataFrame,
    jobs: pd.DataFrame,
    telemetry: pd.DataFrame,
):

    print("\n\n4. CROSS-DATASET INTEGRITY")
    print("=" * 70)

    devices = pd.read_csv(
        DATA_DIR / "devices.csv"
    )

    valid_devices = set(
        devices["device_id"]
    )

    datasets = {
        "calibrations": calibrations,
        "jobs": jobs,
        "telemetry": telemetry,
    }

    for name, df in datasets.items():

        invalid_devices = (
            set(df["device_id"])
            - valid_devices
        )

        assert not invalid_devices, (
            f"{name} contains invalid "
            f"device IDs: {invalid_devices}"
        )

        print(
            f"PASS: {name} device IDs "
            f"match device registry."
        )


def main():

    print("\nqLore Synthetic Data Validation")
    print("=" * 70)

    calibrations, jobs, telemetry = (
        load_data()
    )

    validate_calibration_incident(
        calibrations
    )

    validate_telemetry_incident(
        telemetry
    )

    validate_job_failure_incident(
        jobs
    )

    validate_referential_integrity(
        calibrations,
        jobs,
        telemetry,
    )

    print("\n")
    print("=" * 70)

    print(
        "ALL SYNTHETIC DATA "
        "VALIDATIONS PASSED"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()