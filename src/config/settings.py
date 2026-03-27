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
    data_gold_path: Path
    data_ml_path: Path
    logs_path: Path
    bronze_format: str
    silver_format: str
    gold_format: str
    ml_format: str
    bcb_currencies: list[str]
    bcb_target_countries: list[str]
    bcb_start_date: date | None
    bcb_end_date: date | None
    bcb_timeout_seconds: int
    bcb_retry_attempts: int
    gold_postgres_enabled: bool
    postgres_url: str | None
    postgres_host: str | None
    postgres_port: int
    postgres_db: str | None
    postgres_user: str | None
    postgres_password: str | None
    postgres_schema: str
    postgres_write_mode: str

    def build_postgres_url(self) -> str | None:
        if self.postgres_url:
            return self.postgres_url

        if not all([self.postgres_host, self.postgres_db, self.postgres_user, self.postgres_password]):
            return None

        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )



def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}



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
    data_gold_path = project_root / "data" / "gold"
    data_ml_path = project_root / "data" / "ml"
    logs_path = project_root / "logs"

    return Settings(
        project_root=project_root,
        data_raw_path=Path(os.getenv("DATA_RAW_PATH", data_raw_path)),
        data_bronze_path=Path(os.getenv("DATA_BRONZE_PATH", data_bronze_path)),
        data_silver_path=Path(os.getenv("DATA_SILVER_PATH", data_silver_path)),
        data_gold_path=Path(os.getenv("DATA_GOLD_PATH", data_gold_path)),
        data_ml_path=Path(os.getenv("DATA_ML_PATH", data_ml_path)),
        logs_path=Path(os.getenv("LOG_PATH", logs_path)),
        bronze_format=os.getenv("BRONZE_FORMAT", "parquet").lower(),
        silver_format=os.getenv("SILVER_FORMAT", "parquet").lower(),
        gold_format=os.getenv("GOLD_FORMAT", "parquet").lower(),
        ml_format=os.getenv("ML_FORMAT", "parquet").lower(),
        bcb_currencies=_parse_currencies(os.getenv("BCB_CURRENCIES")),
        bcb_target_countries=_parse_countries(os.getenv("BCB_TARGET_COUNTRIES")),
        bcb_start_date=_parse_date(os.getenv("BCB_START_DATE")),
        bcb_end_date=_parse_date(os.getenv("BCB_END_DATE")),
        bcb_timeout_seconds=int(os.getenv("BCB_TIMEOUT_SECONDS", "20")),
        bcb_retry_attempts=int(os.getenv("BCB_RETRY_ATTEMPTS", "3")),
        gold_postgres_enabled=_parse_bool(os.getenv("GOLD_POSTGRES_ENABLED"), default=False),
        postgres_url=os.getenv("POSTGRES_URL"),
        postgres_host=os.getenv("POSTGRES_HOST"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB"),
        postgres_user=os.getenv("POSTGRES_USER"),
        postgres_password=os.getenv("POSTGRES_PASSWORD"),
        postgres_schema=os.getenv("POSTGRES_SCHEMA", "public"),
        postgres_write_mode=os.getenv("POSTGRES_WRITE_MODE", "replace").strip().lower(),
    )
