from __future__ import annotations

import logging

import pandas as pd

from src.config.settings import Settings
from src.gold.postgres_loader import (
    create_pg_engine,
    create_star_schema_indexes,
    create_star_schema_tables,
    drop_star_schema_tables,
    ensure_schema,
    write_table_controlled,
)
from src.gold.star_schema import GoldStarSchema, build_star_schema
from src.ingestion.file_loader import write_parquet


def _log(logger: logging.Logger, level: int, message: str, **kwargs: object) -> None:
    logger.log(level, message, extra=kwargs)


def run_gold_pipeline(settings: Settings, logger: logging.Logger) -> None:
    silver = _load_silver_inputs(settings)
    star = build_star_schema(
        df_cliente=silver["cliente"],
        df_categoria=silver["categoria"],
        df_meta=silver["meta"],
        df_conta_pagar=silver["conta_pagar"],
        df_conta_receber=silver["conta_receber"],
    )

    _persist_gold_files(settings=settings, star=star)

    _log(
        logger,
        logging.INFO,
        "Gold star schema generated",
        stage="gold",
        dataset="all",
        status="success",
    )

    if not settings.gold_postgres_enabled:
        _log(
            logger,
            logging.INFO,
            "Gold PostgreSQL load disabled by configuration",
            stage="gold",
            dataset="postgres",
            status="skipped",
        )
        return

    postgres_url = settings.build_postgres_url()
    if not postgres_url:
        _log(
            logger,
            logging.WARNING,
            "PostgreSQL configuration is incomplete; skipping Gold load",
            stage="gold",
            dataset="postgres",
            status="skipped",
        )
        return

    _load_gold_to_postgres(settings=settings, logger=logger, star=star, postgres_url=postgres_url)


def _load_silver_inputs(settings: Settings) -> dict[str, pd.DataFrame]:
    return {
        "cliente": _read_dataset(settings, "cliente_silver"),
        "categoria": _read_dataset(settings, "categoria_silver"),
        "meta": _read_dataset(settings, "meta_2025_silver"),
        "conta_pagar": _read_dataset(settings, "conta_pagar_silver"),
        "conta_receber": _read_dataset(settings, "conta_receber_silver"),
    }


def _read_dataset(settings: Settings, base_name: str) -> pd.DataFrame:
    parquet_path = settings.data_silver_path / f"{base_name}.parquet"
    csv_path = settings.data_silver_path / f"{base_name}.csv"

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"Silver dataset not found: {base_name}.parquet/csv")


def _persist_gold_files(settings: Settings, star: GoldStarSchema) -> None:
    write_parquet(star.dim_cliente, settings.data_gold_path / "dim_cliente", settings.gold_format)
    write_parquet(star.dim_categoria, settings.data_gold_path / "dim_categoria", settings.gold_format)
    write_parquet(star.dim_data, settings.data_gold_path / "dim_data", settings.gold_format)
    write_parquet(star.dim_moeda, settings.data_gold_path / "dim_moeda", settings.gold_format)

    write_parquet(star.fato_conta_receber, settings.data_gold_path / "fato_conta_receber", settings.gold_format)
    write_parquet(star.fato_conta_pagar, settings.data_gold_path / "fato_conta_pagar", settings.gold_format)
    write_parquet(star.fato_meta_mensal, settings.data_gold_path / "fato_meta_mensal", settings.gold_format)


def _load_gold_to_postgres(settings: Settings, logger: logging.Logger, star: GoldStarSchema, postgres_url: str) -> None:
    engine = create_pg_engine(postgres_url)
    ensure_schema(engine, settings.postgres_schema)

    mode = settings.postgres_write_mode
    if mode == "replace":
        drop_star_schema_tables(engine=engine, schema=settings.postgres_schema)

    create_star_schema_tables(engine=engine, schema=settings.postgres_schema)

    table_defs = [
        ("dim_cliente", star.dim_cliente, ["id_cliente"]),
        ("dim_categoria", star.dim_categoria, ["codigo_categoria"]),
        ("dim_data", star.dim_data, ["sk_data"]),
        ("dim_moeda", star.dim_moeda, ["codigo_moeda"]),
        ("fato_conta_receber", star.fato_conta_receber, ["id_conta_receber"]),
        ("fato_conta_pagar", star.fato_conta_pagar, ["id_conta_pagar"]),
        ("fato_meta_mensal", star.fato_meta_mensal, ["sk_fato_meta"]),
    ]

    for table_name, df, keys in table_defs:
        written = write_table_controlled(
            engine=engine,
            df=df,
            table_name=table_name,
            schema=settings.postgres_schema,
            mode=mode,
            key_columns=keys,
        )
        _log(
            logger,
            logging.INFO,
            f"Gold table loaded into PostgreSQL: {table_name}",
            stage="gold",
            dataset=table_name,
            status="loaded",
            rows_written=written,
            write_mode=mode,
        )

    create_star_schema_indexes(engine=engine, schema=settings.postgres_schema)
