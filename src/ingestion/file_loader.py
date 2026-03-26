from __future__ import annotations

from pathlib import Path

import pandas as pd

SUPPORTED_EXTENSIONS = {".csv", ".json", ".xlsx", ".xls"}

def list_raw_files(raw_path: Path) -> list[Path]:
    return sorted(
        file
        for file in raw_path.iterdir()
        if file.is_file() and file.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def read_raw_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".json":
        return pd.read_json(file_path)

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)

    raise ValueError(f"Unsupported extension: {suffix}")


def write_parquet(df: pd.DataFrame, output_path: Path, output_format: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "parquet":
        try:
            target = output_path.with_suffix(".parquet")
            df.to_parquet(target, index=False)
            return target
        except Exception:
            target = output_path.with_suffix(".csv")
            df.to_csv(target, index=False)
            return target

    target = output_path.with_suffix(".csv")
    df.to_csv(target, index=False)
    return target
