from datetime import date
from pathlib import Path

import pandas as pd


DEVICES = [
    {
        "device_id": "DEV001",
        "device_name": "qlore_alpha",
        "qubit_count": 50,
        "processor_generation": "QPU_GEN_1",
        "location": "Yorktown",
        "operational_status": "ACTIVE",
        "commission_date": date(2023, 1, 15),
    },
    {
        "device_id": "DEV002",
        "device_name": "qlore_beta",
        "qubit_count": 50,
        "processor_generation": "QPU_GEN_1",
        "location": "Poughkeepsie",
        "operational_status": "ACTIVE",
        "commission_date": date(2023, 5, 20),
    },
    {
        "device_id": "DEV003",
        "device_name": "qlore_gamma",
        "qubit_count": 75,
        "processor_generation": "QPU_GEN_2",
        "location": "Yorktown",
        "operational_status": "ACTIVE",
        "commission_date": date(2024, 2, 10),
    },
    {
        "device_id": "DEV004",
        "device_name": "qlore_delta",
        "qubit_count": 75,
        "processor_generation": "QPU_GEN_2",
        "location": "Poughkeepsie",
        "operational_status": "MAINTENANCE",
        "commission_date": date(2024, 7, 5),
    },
    {
        "device_id": "DEV005",
        "device_name": "qlore_epsilon",
        "qubit_count": 100,
        "processor_generation": "QPU_GEN_3",
        "location": "Yorktown",
        "operational_status": "ACTIVE",
        "commission_date": date(2025, 1, 12),
    },
]


def generate_devices() -> pd.DataFrame:
    """Create the qLore quantum device registry."""
    return pd.DataFrame(DEVICES)


def save_devices(df: pd.DataFrame) -> Path:
    """Persist the device registry as CSV."""
    output_dir = Path("data/generated")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "devices.csv"

    df.to_csv(output_path, index=False)

    return output_path


if __name__ == "__main__":
    devices_df = generate_devices()

    output_path = save_devices(devices_df)

    print("\nqLore Device Registry")
    print("=" * 70)
    print(devices_df.to_string(index=False))

    print(f"\nGenerated devices: {len(devices_df)}")
    print(f"Total simulated qubits: {devices_df['qubit_count'].sum()}")
    print(f"Saved to: {output_path}")