from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


RANDOM_SEED = 44

DAYS_OF_HISTORY = 90

# One telemetry reading every 15 minutes
INTERVAL_MINUTES = 15


def load_devices() -> pd.DataFrame:
    path = Path("data/generated/devices.csv")

    if not path.exists():
        raise FileNotFoundError(
            "devices.csv not found. "
            "Run device_generator.py first."
        )

    return pd.read_csv(path)


def generate_telemetry(
    devices_df: pd.DataFrame,
) -> pd.DataFrame:

    rng = np.random.default_rng(
        RANDOM_SEED
    )

    end_time = datetime.now().replace(
        second=0,
        microsecond=0,
    )

    start_time = (
        end_time
        - timedelta(days=DAYS_OF_HISTORY)
    )

    timestamps = pd.date_range(
        start=start_time,
        end=end_time,
        freq=f"{INTERVAL_MINUTES}min",
    )

    records = []

    telemetry_id = 1

    for _, device in devices_df.iterrows():

        device_id = device["device_id"]

        # Each device gets slightly different baselines
        base_temperature = rng.uniform(
            12.0,
            16.0,
        )

        base_cpu = rng.uniform(
            35,
            55,
        )

        base_memory = rng.uniform(
            40,
            60,
        )

        base_queue = rng.integers(
            15,
            45,
        )

        base_snr = rng.uniform(
            25,
            35,
        )

        uptime_hours = rng.uniform(
            100,
            500,
        )

        for timestamp in timestamps:

            # -------------------------
            # Normal operating behavior
            # -------------------------

            temperature = (
                base_temperature
                + rng.normal(0, 0.7)
            )

            cpu_usage = (
                base_cpu
                + rng.normal(0, 10)
            )

            memory_usage = (
                base_memory
                + rng.normal(0, 8)
            )

            queue_depth = int(
                max(
                    0,
                    base_queue
                    + rng.normal(0, 12),
                )
            )

            active_jobs = int(
                max(
                    0,
                    queue_depth
                    * rng.uniform(0.20, 0.60),
                )
            )

            signal_noise_ratio = (
                base_snr
                + rng.normal(0, 1.5)
            )

            uptime_hours += (
                INTERVAL_MINUTES / 60
            )

            # -------------------------
            # Controlled anomaly
            # DEV002 final 10 days
            # -------------------------

            anomaly_start = (
                end_time
                - timedelta(days=10)
            )

            if (
                device_id == "DEV002"
                and timestamp >= anomaly_start
            ):

                elapsed_days = (
                    timestamp - anomaly_start
                ).total_seconds() / 86400

                # Increasing workload pressure
                # Increasing workload pressure
                queue_depth += int(
                    elapsed_days * 6
                )

                active_jobs += int(
                    elapsed_days * 2
                )

                # Stronger signal degradation
                signal_noise_ratio -= (
                    elapsed_days * 1.2
                )

                # Gradual thermal instability
                temperature += (
                    elapsed_days * 0.35
                )

                # Slight thermal instability
                temperature += (
                    elapsed_days * 0.12
                )

            # -------------------------
            # Clamp unrealistic values
            # -------------------------

            temperature = max(
                temperature,
                5,
            )

            cpu_usage = np.clip(
                cpu_usage,
                0,
                100,
            )

            memory_usage = np.clip(
                memory_usage,
                0,
                100,
            )

            signal_noise_ratio = max(
                signal_noise_ratio,
                0,
            )

            # -------------------------
            # Determine system health
            # -------------------------

            system_status = "HEALTHY"

            if (
                temperature > 22
                or signal_noise_ratio < 20
                or queue_depth > 90
            ):
                system_status = "DEGRADED"

            if (
                temperature > 28
                or signal_noise_ratio < 15
                or queue_depth > 130
            ):
                system_status = "CRITICAL"

            records.append(
                {
                    "telemetry_id": (
                        f"TEL{telemetry_id:09d}"
                    ),
                    "device_id": device_id,
                    "event_timestamp": timestamp,
                    "temperature_mk": round(
                        temperature,
                        3,
                    ),
                    "cpu_usage_pct": round(
                        float(cpu_usage),
                        2,
                    ),
                    "memory_usage_pct": round(
                        float(memory_usage),
                        2,
                    ),
                    "queue_depth": queue_depth,
                    "active_jobs": active_jobs,
                    "signal_noise_ratio": round(
                        signal_noise_ratio,
                        3,
                    ),
                    "uptime_hours": round(
                        uptime_hours,
                        2,
                    ),
                    "system_status": system_status,

                    # Important later for Kafka
                    "schema_version": "v1",
                }
            )

            telemetry_id += 1

    return pd.DataFrame(records)


def save_telemetry(
    telemetry_df: pd.DataFrame,
) -> Path:

    output_dir = Path(
        "data/generated"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / "telemetry.csv"
    )

    telemetry_df.to_csv(
        output_path,
        index=False,
    )

    return output_path


def print_summary(
    telemetry_df: pd.DataFrame,
):

    print(
        "\nqLore Device Telemetry Dataset"
    )

    print("=" * 80)

    print(
        f"Telemetry events: "
        f"{len(telemetry_df):,}"
    )

    print(
        f"Devices: "
        f"{telemetry_df['device_id'].nunique()}"
    )

    print("\nSystem status:")

    print(
        telemetry_df[
            "system_status"
        ].value_counts()
    )

    print(
        "\nStatus by device:"
    )

    print(
        pd.crosstab(
            telemetry_df["device_id"],
            telemetry_df["system_status"],
        )
    )


if __name__ == "__main__":

    devices_df = load_devices()

    telemetry_df = generate_telemetry(
        devices_df
    )

    output_path = save_telemetry(
        telemetry_df
    )

    print_summary(
        telemetry_df
    )

    print(
        f"\nSaved to: {output_path}"
    )