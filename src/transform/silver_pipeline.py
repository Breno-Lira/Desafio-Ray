from __future__ import annotations

import logging

import pandas as pd

from src.config.settings import Settings
from src.ingestion.file_loader import write_bronze
from src.transform.meta_transform import transform_meta_table


def _log(logger: logging.Logger, level: int, message: str, **kwargs: object) -> None:
	logger.log(level, message, extra=kwargs)


def run_silver_pipeline(settings: Settings, logger: logging.Logger, table_name: str | None = None) -> None:
	table = (table_name or "metas").strip().lower()

	if table != "metas":
		raise ValueError("For now, only table_name='metas' is supported in Silver")

	_run_metas_silver(settings=settings, logger=logger)


def _run_metas_silver(settings: Settings, logger: logging.Logger) -> None:
	bronze_file = _resolve_bronze_file(settings=settings, base_name="PS_Meta2025")

	if bronze_file is None:
		_log(
			logger,
			logging.ERROR,
			"Bronze file for metas not found",
			stage="silver",
			dataset="metas",
			status="error",
			error="Missing PS_Meta2025.parquet/csv in data/bronze",
		)
		return

	df_bronze = _read_bronze_file(bronze_file)
	df_silver = transform_meta_table(df_bronze)

	output_file = write_bronze(
		df=df_silver,
		output_path=settings.data_silver_path / "meta_2025_silver",
		output_format=settings.silver_format,
	)

	valid_mask = (
		df_silver["data_mm_yyyy"].notna()
		& df_silver["meta_valor"].notna()
		& df_silver["mes"].notna()
		& df_silver["ano"].notna()
		& df_silver["nome_mes"].notna()
	)
	valid_records = int(valid_mask.sum())
	invalid_records = int((~valid_mask).sum())

	_log(
		logger,
		logging.INFO,
		"Silver metas transformation completed",
		stage="silver",
		dataset="metas",
		status="success",
	)
	_log(
		logger,
		logging.INFO,
		f"Rows={len(df_silver)} Valid={valid_records} Invalid={invalid_records} Output={output_file.name}",
		stage="silver",
		dataset="metas",
		status="metadata",
	)


def _resolve_bronze_file(settings: Settings, base_name: str) -> str | None:
	parquet_path = settings.data_bronze_path / f"{base_name}.parquet"
	csv_path = settings.data_bronze_path / f"{base_name}.csv"

	if parquet_path.exists():
		return str(parquet_path)
	if csv_path.exists():
		return str(csv_path)
	return None


def _read_bronze_file(file_path: str) -> pd.DataFrame:
	if file_path.endswith(".parquet"):
		return pd.read_parquet(file_path)
	return pd.read_csv(file_path)
