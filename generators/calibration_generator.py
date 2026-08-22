from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


RANDOM_SEED = 42
DAYS_OF_HISTORY = 90


def load_devices() -> pd.DataFrame:
    device_path = Path("data/generated/devices.csv")

    if not device_path.exists():
        raise FileNotFoundError(
            "devices.csv not found. Run device_generator.py first."
        )

    return pd.read_csv(device_path)


def generate_calibrations(
    devices_df: pd.DataFrame,
    days: int = DAYS_OF_HISTORY,
) -> pd.DataFrame:

    rng = np.random.default_rng(RANDOM_SEED)

    records = []

    end_date = datetime.now().replace(
        hour=6,
        minute=0,
        second=0,
        microsecond=0,
    )

    start_date = end_date - timedelta(days=days - 1)

    calibration_id = 1

    for _, device in devices_df.iterrows():

        device_id = device["device_id"]
        qubit_count = int(device["qubit_count"])

        for qubit_number in range(qubit_count):

            qubit_id = f"Q{qubit_number:03d}"

            # Give every qubit its own baseline characteristics
            base_t1 = rng.uniform(80, 180)
            base_t2 = rng.uniform(60, 150)

            base_gate_error = rng.uniform(0.0005, 0.004)
            base_readout_error = rng.uniform(0.005, 0.04)

            base_frequency = rng.uniform(4.5, 5.5)

            for day_offset in range(days):

                timestamp = start_date + timedelta(days=day_offset)

                # Normal day-to-day measurement variation
                t1 = base_t1 + rng.normal(0, 5)
                t2 = base_t2 + rng.normal(0, 5)

                gate_error = (
                    base_gate_error
                    + rng.normal(0, 0.0002)
                )

                readout_error = (
                    base_readout_error
                    + rng.normal(0, 0.002)
                )

                frequency_ghz = (
                    base_frequency
                    + rng.normal(0, 0.01)
                )

                # Keep values physically sensible for our simulation
                t1 = max(t1, 10)
                t2 = max(t2, 10)

                gate_error = max(gate_error, 0)
                readout_error = max(readout_error, 0)

                calibration_status = "PASS"

                if (
                    gate_error > 0.01
                    or readout_error > 0.08
                    or t1 < 40
                    or t2 < 30
                ):
                    calibration_status = "FAIL"

                records.append(
                    {
                        "calibration_id": f"CAL{calibration_id:08d}",
                        "device_id": device_id,
                        "qubit_id": qubit_id,
                        "calibration_timestamp": timestamp,
                        "t1_us": round(t1, 3),
                        "t2_us": round(t2, 3),
                        "gate_error": round(gate_error, 6),
                        "readout_error": round(readout_error, 6),
                        "frequency_ghz": round(frequency_ghz, 6),
                        "calibration_status": calibration_status,
                    }
                )

                calibration_id += 1

    return pd.DataFrame(records)


def inject_calibration_drift(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Inject controlled degradation into qlore_beta / DEV002
    so we have a known anomaly for later pipeline and AI testing.
    """

    drift_device = "DEV002"
    drift_qubit = "Q017"

    device_mask = (
        (df["device_id"] == drift_device)
        & (df["qubit_id"] == drift_qubit)
    )

    affected_rows = df.loc[device_mask].sort_values(
        "calibration_timestamp"
    )

    if affected_rows.empty:
        return df

    # Last 10 calibration records will degrade gradually
    drift_indexes = affected_rows.tail(10).index

    drift_strength = np.linspace(1, 6, len(drift_indexes))

    for index, strength in zip(
        drift_indexes,
        drift_strength,
    ):

        df.loc[index, "gate_error"] *= strength
        df.loc[index, "readout_error"] *= (
            1 + (strength - 1) * 0.8
        )

        df.loc[index, "t1_us"] *= (
            1 - (strength - 1) * 0.12
        )

        df.loc[index, "t2_us"] *= (
            1 - (strength - 1) * 0.15
        )

    df["gate_error"] = df["gate_error"].round(6)
    df["readout_error"] = df["readout_error"].round(6)

    df["t1_us"] = df["t1_us"].round(3)
    df["t2_us"] = df["t2_us"].round(3)

    # Recalculate status after anomaly injection
    df["calibration_status"] = np.where(
        (df["gate_error"] > 0.01)
        | (df["readout_error"] > 0.08)
        | (df["t1_us"] < 40)
        | (df["t2_us"] < 30),
        "FAIL",
        "PASS",
    )

    return df


def save_calibrations(
    df: pd.DataFrame,
) -> Path:

    output_dir = Path("data/generated")
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / "calibrations.csv"
    )

    df.to_csv(
        output_path,
        index=False,
    )

    return output_path


if __name__ == "__main__":

    devices_df = load_devices()

    calibrations_df = generate_calibrations(
        devices_df
    )

    calibrations_df = inject_calibration_drift(
        calibrations_df
    )

    output_path = save_calibrations(
        calibrations_df
    )

    print("\nqLore Calibration Dataset")
    print("=" * 80)

    print(
        calibrations_df.head().to_string(
            index=False
        )
    )

    print(
        f"\nCalibration records: "
        f"{len(calibrations_df):,}"
    )

    print(
        f"Devices: "
        f"{calibrations_df['device_id'].nunique()}"
    )

    print(
        f"Unique qubits: "
        f"{calibrations_df[['device_id', 'qubit_id']].drop_duplicates().shape[0]}"
    )

    print(
        "\nCalibration status:"
    )

    print(
        calibrations_df[
            "calibration_status"
        ].value_counts()
    )

    print(
        f"\nSaved to: {output_path}"
    )

    print(
        "\nInjected anomaly: "
        "DEV002 / Q017"
    )