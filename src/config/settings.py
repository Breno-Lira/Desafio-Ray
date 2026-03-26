from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_raw_path: Path
    data_bronze_path: Path
    data_silver_path: Path
    logs_path: Path
    bronze_format: str
    silver_format: str
    bcb_currencies: list[str]
    bcb_target_countries: list[str]
    bcb_start_date: date | None
    bcb_end_date: date | None
    bcb_timeout_seconds: int
    bcb_retry_attempts: int



def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)



def _parse_currencies(value: str | None) -> list[str]:
    if not value:
        return ["USD", "EUR", "GBP", "ARS", "CAD"]
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _parse_countries(value: str | None) -> list[str]:
    if not value:
        return [
            "PORTUGAL",
            "FRANCA",
            "ESTADOS UNIDOS",
            "CANADA",
            "ARGENTINA",
            "REINO UNIDO",
        ]
    return [item.strip().upper() for item in value.split(",") if item.strip()]



def get_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[2]

    data_raw_path = project_root / "data" / "raw"
    data_bronze_path = project_root / "data" / "bronze"
    data_silver_path = project_root / "data" / "silver"
    logs_path = project_root / "logs"

    return Settings(
        project_root=project_root,
        data_raw_path=Path(os.getenv("DATA_RAW_PATH", data_raw_path)),
        data_bronze_path=Path(os.getenv("DATA_BRONZE_PATH", data_bronze_path)),
        data_silver_path=Path(os.getenv("DATA_SILVER_PATH", data_silver_path)),
        logs_path=Path(os.getenv("LOG_PATH", logs_path)),
        bronze_format=os.getenv("BRONZE_FORMAT", "parquet").lower(),
        silver_format=os.getenv("SILVER_FORMAT", "parquet").lower(),
        bcb_currencies=_parse_currencies(os.getenv("BCB_CURRENCIES")),
        bcb_target_countries=_parse_countries(os.getenv("BCB_TARGET_COUNTRIES")),
        bcb_start_date=_parse_date(os.getenv("BCB_START_DATE")),
        bcb_end_date=_parse_date(os.getenv("BCB_END_DATE")),
        bcb_timeout_seconds=int(os.getenv("BCB_TIMEOUT_SECONDS", "20")),
        bcb_retry_attempts=int(os.getenv("BCB_RETRY_ATTEMPTS", "3")),
    )
