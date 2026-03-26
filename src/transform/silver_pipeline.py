from __future__ import annotations

import logging

import pandas as pd

from src.transform.client_transform import transform_client_table
from src.config.settings import Settings
from src.ingestion.file_loader import write_parquet
from src.transform.category_transform import transform_category_table
from src.transform.meta_transform import transform_meta_table
from src.transform.conta_pagar_transform import transform_conta_pagar_table
from src.transform.conta_receber_transform import transform_conta_receber_table


def _log(logger: logging.Logger, level: int, message: str, **kwargs: object) -> None:
	logger.log(level, message, extra=kwargs)


def run_silver_pipeline(settings: Settings, logger: logging.Logger, table_name: str | None = None) -> None:
	table = (table_name or "all").strip().lower()

	if table in {"all", "todas", "todos"}:
		_run_metas_silver(settings=settings, logger=logger)
		_run_categoria_silver(settings=settings, logger=logger)
		_run_cliente_silver(settings=settings, logger=logger)
		_run_conta_pagar_silver(settings=settings, logger=logger)
		_run_conta_receber_silver(settings=settings, logger=logger)
		return

	if table in {"metas", "meta"}:
		_run_metas_silver(settings=settings, logger=logger)
		return

	if table in {"categoria", "categorias"}:
		_run_categoria_silver(settings=settings, logger=logger)
		return

	if table in {"cliente", "clientes"}:
		_run_cliente_silver(settings=settings, logger=logger)
		return

	if table in {"conta_pagar", "contas_pagar", "pagar"}:
		_run_conta_pagar_silver(settings=settings, logger=logger)
		return

	if table in {"conta_receber", "contas_receber", "receber"}:
		_run_conta_receber_silver(settings=settings, logger=logger)
		return

	raise ValueError(
		"Supported silver tables are: metas, categoria, cliente, conta_pagar, conta_receber, or 'all'"
	)


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

	output_file = write_parquet(
		df=df_silver,
		output_path=settings.data_silver_path / "meta_2025_silver",
		output_format=settings.silver_format,
	)

	valid_mask = (
		df_silver["mes_ano_meta"].notna()
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


def _run_categoria_silver(settings: Settings, logger: logging.Logger) -> None:
	bronze_file = _resolve_bronze_file(settings=settings, base_name="PS_Categoria")

	if bronze_file is None:
		_log(
			logger,
			logging.ERROR,
			"Bronze file for categoria not found",
			stage="silver",
			dataset="categoria",
			status="error",
			error="Missing PS_Categoria.parquet/csv in data/bronze",
		)
		return

	df_bronze = _read_bronze_file(bronze_file)
	rows_before = len(df_bronze)
	df_silver = transform_category_table(df_bronze)
	rows_after = len(df_silver)

	output_file = write_parquet(
		df=df_silver,
		output_path=settings.data_silver_path / "categoria_silver",
		output_format=settings.silver_format,
	)

	removed_records = int(rows_before - rows_after)

	_log(
		logger,
		logging.INFO,
		"Silver categoria transformation completed",
		stage="silver",
		dataset="categoria",
		status="success",
	)
	_log(
		logger,
		logging.INFO,
		f"RowsBefore={rows_before} RowsAfter={rows_after} Removed={removed_records} Output={output_file.name}",
		stage="silver",
		dataset="categoria",
		status="metadata",
	)


def _run_cliente_silver(settings: Settings, logger: logging.Logger) -> None:
	bronze_file = _resolve_bronze_file(settings=settings, base_name="PS_Cliente")

	if bronze_file is None:
		_log(
			logger,
			logging.ERROR,
			"Bronze file for cliente not found",
			stage="silver",
			dataset="cliente",
			status="error",
			error="Missing PS_Cliente.parquet/csv in data/bronze",
		)
		return

	df_bronze = _read_bronze_file(bronze_file)
	rows_before = len(df_bronze)
	df_silver = transform_client_table(df_bronze)
	rows_after = len(df_silver)

	output_file = write_parquet(
		df=df_silver,
		output_path=settings.data_silver_path / "cliente_silver",
		output_format=settings.silver_format,
	)

	missing_country = int(df_silver["pais_cliente"].isna().sum())
	missing_currency = int(df_silver["moeda_pais"].isna().sum())
	missing_date = int(df_silver["data_cadastro_cliente"].isna().sum())

	_log(
		logger,
		logging.INFO,
		"Silver cliente transformation completed",
		stage="silver",
		dataset="cliente",
		status="success",
	)
	_log(
		logger,
		logging.INFO,
		(
			f"RowsBefore={rows_before} RowsAfter={rows_after} "
			f"MissingCountry={missing_country} MissingCurrency={missing_currency} "
			f"MissingDate={missing_date} Output={output_file.name}"
		),
		stage="silver",
		dataset="cliente",
		status="metadata",
	)


def _run_conta_pagar_silver(settings: Settings, logger: logging.Logger) -> None:
	bronze_file = _resolve_bronze_file(settings=settings, base_name="PS_Conta_Pagar")

	if bronze_file is None:
		_log(
			logger,
			logging.ERROR,
			"Bronze file for conta_pagar not found",
			stage="silver",
			dataset="conta_pagar",
			status="error",
			error="Missing PS_Conta_Pagar.parquet/csv in data/bronze",
		)
		return

	df_bronze = _read_bronze_file(bronze_file)
	rows_before = len(df_bronze)
	df_silver = transform_conta_pagar_table(df_bronze)
	rows_after = len(df_silver)

	output_file = write_parquet(
		df=df_silver,
		output_path=settings.data_silver_path / "conta_pagar_silver",
		output_format=settings.silver_format,
	)

	paid_count = int(df_silver["status_conta"].astype(str).str.lower().eq("pago").sum())
	open_count = int(df_silver["status_conta"].astype(str).str.lower().eq("em aberto").sum())

	_log(
		logger,
		logging.INFO,
		"Silver conta_pagar transformation completed",
		stage="silver",
		dataset="conta_pagar",
		status="success",
	)
	_log(
		logger,
		logging.INFO,
		(
			f"RowsBefore={rows_before} RowsAfter={rows_after} "
			f"Pago={paid_count} EmAberto={open_count} "
			f"Output={output_file.name}"
		),
		stage="silver",
		dataset="conta_pagar",
		status="metadata",
	)


def _run_conta_receber_silver(settings: Settings, logger: logging.Logger) -> None:
	bronze_file = _resolve_bronze_file(settings=settings, base_name="PS_Conta_Receber")

	if bronze_file is None:
		_log(
			logger,
			logging.ERROR,
			"Bronze file for conta_receber not found",
			stage="silver",
			dataset="conta_receber",
			status="error",
			error="Missing PS_Conta_Receber.parquet/csv in data/bronze",
		)
		return

	df_bcb: pd.DataFrame | None = None
	bcb_file = settings.data_bronze_path / "bcb_cotacoes.parquet"
	if bcb_file.exists():
		df_bcb = _read_bronze_file(str(bcb_file))

	df_cliente: pd.DataFrame | None = None
	cliente_file = settings.data_silver_path / "cliente_silver.parquet"
	if cliente_file.exists():
		df_cliente = _read_bronze_file(str(cliente_file))

	df_bronze = _read_bronze_file(bronze_file)
	rows_before = len(df_bronze)
	df_silver = transform_conta_receber_table(df_bronze, df_bcb=df_bcb, df_cliente=df_cliente)
	rows_after = len(df_silver)

	output_file = write_parquet(
		df=df_silver,
		output_path=settings.data_silver_path / "conta_receber_silver",
		output_format=settings.silver_format,
	)

	status_series = df_silver["status_conta"].astype(str).str.strip().str.lower()
	recebido_count = int(status_series.str.contains("recebido|pago", regex=True, na=False).sum())
	em_aberto_count = int(status_series.str.contains("aberto|pendente", regex=True, na=False).sum())

	_log(
		logger,
		logging.INFO,
		"Silver conta_receber transformation completed",
		stage="silver",
		dataset="conta_receber",
		status="success",
	)
	_log(
		logger,
		logging.INFO,
		(
			f"RowsBefore={rows_before} RowsAfter={rows_after} "
			f"Recebido={recebido_count} EmAberto={em_aberto_count} "
			f"Output={output_file.name}"
		),
		stage="silver",
		dataset="conta_receber",
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
