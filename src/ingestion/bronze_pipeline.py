from __future__ import annotations

import logging
from pathlib import Path

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

    df_rates = client.fetch_rates(
        currencies=target_currencies,
        start_date=settings.bcb_start_date,
        end_date=settings.bcb_end_date,
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


def _get_target_currencies_from_countries(settings: Settings) -> list[str]:
    currencies = {
        CURRENCY_BY_COUNTRY[country]
        for country in settings.bcb_target_countries
        if country in CURRENCY_BY_COUNTRY and CURRENCY_BY_COUNTRY[country] != "BRL"
    }
    return sorted(currencies)
