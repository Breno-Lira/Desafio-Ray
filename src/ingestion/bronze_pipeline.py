from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.config.settings import Settings
from src.ingestion.bcb_client import BcbClient, BcbClientConfig
from src.ingestion.file_loader import list_raw_files, read_raw_file, write_parquet
from src.transform.client_transform import CURRENCY_BY_COUNTRY


def _log(logger: logging.Logger, level: int, message: str, **kwargs: object) -> None:
    logger.log(level, message, extra=kwargs)


def run_bronze_ingestion(settings: Settings, logger: logging.Logger) -> None:
    raw_files = list_raw_files(settings.data_raw_path)

    if not raw_files:
        _log(
            logger,
            logging.WARNING,
            "No raw files were found",
            stage="bronze",
            dataset="raw",
            status="skipped",
        )
        return

    for file_path in raw_files:
        dataset_name = file_path.stem
        try:
            df = read_raw_file(file_path)
            target_base = settings.data_bronze_path / dataset_name
            output_file = write_parquet(df, target_base, settings.bronze_format)

            _log(
                logger,
                logging.INFO,
                "Raw dataset ingested into bronze",
                stage="bronze",
                dataset=dataset_name,
                status="success",
            )
            _log(
                logger,
                logging.INFO,
                f"Rows={len(df)} Output={output_file.name}",
                stage="bronze",
                dataset=dataset_name,
                status="metadata",
            )
        except Exception as exc:
            _log(
                logger,
                logging.ERROR,
                "Failed to ingest raw dataset",
                stage="bronze",
                dataset=dataset_name,
                status="error",
                error=str(exc),
            )

    _ingest_bcb_rates(settings=settings, logger=logger)



def _ingest_bcb_rates(settings: Settings, logger: logging.Logger) -> None:
    target_currencies = _get_target_currencies_from_countries(settings)

    if not target_currencies:
        _log(
            logger,
            logging.WARNING,
            "No target currencies resolved from requested countries",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="skipped",
        )
        return

    _log(
        logger,
        logging.INFO,
        f"Using BCB currencies from countries={settings.bcb_target_countries}: {target_currencies}",
        stage="bronze",
        dataset="bcb_cotacoes",
        status="metadata",
    )

    client = BcbClient(
        BcbClientConfig(
            timeout_seconds=settings.bcb_timeout_seconds,
            retry_attempts=settings.bcb_retry_attempts,
        )
    )

    quote_dates = _build_bcb_quote_dates(settings=settings, logger=logger)
    if not quote_dates:
        _log(
            logger,
            logging.WARNING,
            "No valid quote dates resolved for BCB ingestion",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="skipped",
        )
        return

    df_rates = client.fetch_rates_for_dates(
        currencies=target_currencies,
        quote_dates=quote_dates,
    )

    if df_rates.empty:
        _log(
            logger,
            logging.WARNING,
            "No BCB rates were returned for the selected window",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="skipped",
        )
        return

    rows_original = len(df_rates)
    df_rates = _complete_missing_quote_days(
        df_rates=df_rates,
        quote_dates=quote_dates,
        currencies=target_currencies,
    )
    rows_completed = len(df_rates)

    output_file = write_parquet(
        df=df_rates,
        output_path=settings.data_bronze_path / "bcb_cotacoes",
        output_format=settings.bronze_format,
    )

    _log(
        logger,
        logging.INFO,
        f"BCB rates ingested Rows={len(df_rates)} Output={Path(output_file).name}",
        stage="bronze",
        dataset="bcb_cotacoes",
        status="success",
    )
    _log(
        logger,
        logging.INFO,
        f"RowsOriginal={rows_original} RowsCompleted={rows_completed}",
        stage="bronze",
        dataset="bcb_cotacoes",
        status="metadata",
    )


def _get_target_currencies_from_countries(settings: Settings) -> list[str]:
    currencies = {
        CURRENCY_BY_COUNTRY[country]
        for country in settings.bcb_target_countries
        if country in CURRENCY_BY_COUNTRY and CURRENCY_BY_COUNTRY[country] != "BRL"
    }
    return sorted(currencies)


def _build_bcb_quote_dates(settings: Settings, logger: logging.Logger) -> list[date]:
    months = _extract_conta_receber_payment_months(settings=settings, logger=logger)

    if not months:
        if settings.bcb_start_date is None or settings.bcb_end_date is None:
            _log(
                logger,
                logging.WARNING,
                (
                    "No payment months found and BCB_START_DATE/BCB_END_DATE are not set; "
                    "unable to build BCB date window"
                ),
                stage="bronze",
                dataset="bcb_cotacoes",
                status="metadata",
            )
            return []

        fallback_dates = _build_daily_range(settings.bcb_start_date, settings.bcb_end_date)
        _log(
            logger,
            logging.INFO,
            (
                "Using fallback continuous BCB date range because no valid "
                "conta_receber payment months were found"
            ),
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )
        return fallback_dates

    quote_dates: list[date] = []
    sorted_months = sorted(months)

    auto_start = date(sorted_months[0][0], sorted_months[0][1], 1)
    auto_end = _last_day_of_month(sorted_months[-1][0], sorted_months[-1][1])

    effective_start = settings.bcb_start_date or auto_start
    effective_end = settings.bcb_end_date or auto_end

    if effective_end < effective_start:
        _log(
            logger,
            logging.WARNING,
            (
                f"Invalid BCB date bounds after resolution: start={effective_start} "
                f"end={effective_end}"
            ),
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )
        return []

    if settings.bcb_start_date is None or settings.bcb_end_date is None:
        _log(
            logger,
            logging.INFO,
            (
                f"Auto-resolved BCB bounds from conta_receber payments: "
                f"start={effective_start} end={effective_end}"
            ),
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )

    for year, month in sorted_months:
        month_start = date(year, month, 1)
        month_end = _last_day_of_month(year, month)

        window_start = max(month_start, effective_start)
        window_end = min(month_end, effective_end)

        if window_start <= window_end:
            quote_dates.extend(_build_daily_range(window_start, window_end))

    unique_dates = sorted(set(quote_dates))
    _log(
        logger,
        logging.INFO,
        (
            f"BCB query optimized using {len(sorted_months)} payment month(s) "
            f"and {len(unique_dates)} quote day(s)"
        ),
        stage="bronze",
        dataset="bcb_cotacoes",
        status="metadata",
    )
    return unique_dates


def _extract_conta_receber_payment_months(settings: Settings, logger: logging.Logger) -> set[tuple[int, int]]:
    source_file = _resolve_conta_receber_source_file(settings)
    if source_file is None:
        _log(
            logger,
            logging.WARNING,
            "Conta receber source file not found for BCB date optimization",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )
        return set()

    try:
        if source_file.suffix.lower() == ".parquet":
            df = pd.read_parquet(source_file)
        elif source_file.suffix.lower() == ".csv":
            df = pd.read_csv(source_file)
        else:
            df = read_raw_file(source_file)
    except Exception as exc:
        _log(
            logger,
            logging.WARNING,
            f"Failed to read conta_receber source for BCB optimization: {exc}",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )
        return set()

    if "data_pagamento" not in df.columns:
        _log(
            logger,
            logging.WARNING,
            "Column data_pagamento not found in conta_receber source",
            stage="bronze",
            dataset="bcb_cotacoes",
            status="metadata",
        )
        return set()

    parsed = _parse_mixed_date_series(df["data_pagamento"])
    parsed = parsed.dropna()
    if parsed.empty:
        return set()

    return {(int(item.year), int(item.month)) for item in parsed}


def _resolve_conta_receber_source_file(settings: Settings) -> Path | None:
    bronze_candidates = [
        settings.data_bronze_path / "PS_Conta_Receber.parquet",
        settings.data_bronze_path / "PS_Conta_Receber.csv",
    ]
    for file_path in bronze_candidates:
        if file_path.exists():
            return file_path

    for raw_file in list_raw_files(settings.data_raw_path):
        if raw_file.stem.lower() == "ps_conta_receber":
            return raw_file

    return None


def _parse_mixed_date_series(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    text = text.mask(text.str.lower().isin(["", "nan", "none", "nat"]))

    parsed = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    missing = parsed.isna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(text.loc[missing], format="%d/%m/%Y", errors="coerce")

    missing = parsed.isna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(text.loc[missing], format="%m-%d-%Y", errors="coerce")

    return parsed


def _build_daily_range(start_date: date, end_date: date) -> list[date]:
    dates: list[date] = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _last_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _complete_missing_quote_days(
    df_rates: pd.DataFrame,
    quote_dates: list[date],
    currencies: list[str],
) -> pd.DataFrame:
    if df_rates.empty:
        return df_rates

    all_dates = pd.to_datetime(sorted(set(quote_dates)))
    if all_dates.empty:
        return df_rates

    normalized = df_rates.copy()
    normalized["data"] = pd.to_datetime(normalized["data"], errors="coerce")
    normalized = normalized.dropna(subset=["data"]).copy()

    if normalized.empty:
        return normalized

    normalized = normalized.sort_values(["moeda", "data"])
    normalized = normalized.drop_duplicates(subset=["moeda", "data"], keep="last")

    full_index = pd.MultiIndex.from_product(
        [sorted(currencies), all_dates],
        names=["moeda", "data"],
    )

    completed = normalized.set_index(["moeda", "data"]).reindex(full_index)
    completed[["cotacao_compra", "cotacao_venda"]] = completed.groupby(level=0)[
        ["cotacao_compra", "cotacao_venda"]
    ].ffill()

    completed = completed.dropna(subset=["cotacao_compra", "cotacao_venda"]) 
    completed = completed.reset_index()
    completed["data"] = completed["data"].dt.strftime("%Y-%m-%d")

    return completed
