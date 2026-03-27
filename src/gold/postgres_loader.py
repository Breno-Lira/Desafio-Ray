from __future__ import annotations

from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


def create_pg_engine(postgres_url: str) -> Engine:
    return create_engine(postgres_url, pool_pre_ping=True)


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def create_star_schema_tables(engine: Engine, schema: str) -> None:
    ddl_statements = [
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."dim_cliente" (
            id_cliente BIGINT PRIMARY KEY,
            nome_cliente TEXT,
            pais_cliente TEXT,
            moeda_pais TEXT,
            data_cadastro_cliente DATE
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."dim_categoria" (
            sk_categoria BIGINT PRIMARY KEY,
            codigo_categoria TEXT UNIQUE,
            nome_categoria TEXT,
            tipo_categoria TEXT
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."dim_data" (
            sk_data BIGINT PRIMARY KEY,
            data_calendario DATE,
            ano BIGINT,
            trimestre BIGINT,
            mes BIGINT,
            nome_mes TEXT,
            dia BIGINT,
            dia_semana_num BIGINT,
            nome_dia_semana TEXT,
            fim_de_semana BOOLEAN
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."dim_moeda" (
            sk_moeda BIGINT PRIMARY KEY,
            codigo_moeda TEXT UNIQUE
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."fato_conta_receber" (
            id_conta_receber BIGINT PRIMARY KEY,
            id_cliente BIGINT REFERENCES "{schema}"."dim_cliente"(id_cliente),
            sk_categoria BIGINT REFERENCES "{schema}"."dim_categoria"(sk_categoria),
            sk_moeda BIGINT REFERENCES "{schema}"."dim_moeda"(sk_moeda),
            sk_data_pagamento BIGINT REFERENCES "{schema}"."dim_data"(sk_data),
            sk_data_vencimento BIGINT REFERENCES "{schema}"."dim_data"(sk_data),
            valor_conta DOUBLE PRECISION,
            valor_conta_brl DOUBLE PRECISION,
            status_conta TEXT,
            observacao_conta TEXT
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."fato_conta_pagar" (
            id_conta_pagar BIGINT PRIMARY KEY,
            sk_categoria BIGINT REFERENCES "{schema}"."dim_categoria"(sk_categoria),
            sk_moeda BIGINT REFERENCES "{schema}"."dim_moeda"(sk_moeda),
            sk_data_pagamento BIGINT REFERENCES "{schema}"."dim_data"(sk_data),
            sk_data_vencimento BIGINT REFERENCES "{schema}"."dim_data"(sk_data),
            valor_conta DOUBLE PRECISION,
            valor_conta_brl DOUBLE PRECISION,
            status_conta TEXT,
            destino_pagamento_conta TEXT,
            observacao_conta TEXT
        )
        ''',
        f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."fato_meta_mensal" (
            sk_fato_meta BIGINT PRIMARY KEY,
            sk_data_referencia BIGINT REFERENCES "{schema}"."dim_data"(sk_data),
            meta_valor DOUBLE PRECISION,
            mes BIGINT,
            ano BIGINT,
            nome_mes TEXT
        )
        ''',
    ]

    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))


def create_star_schema_indexes(engine: Engine, schema: str) -> None:
    index_statements = [
        f'CREATE INDEX IF NOT EXISTS "idx_fato_receber_id_cliente" ON "{schema}"."fato_conta_receber" ("id_cliente")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_receber_sk_categoria" ON "{schema}"."fato_conta_receber" ("sk_categoria")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_receber_sk_moeda" ON "{schema}"."fato_conta_receber" ("sk_moeda")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_receber_sk_data_pagamento" ON "{schema}"."fato_conta_receber" ("sk_data_pagamento")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_receber_sk_data_vencimento" ON "{schema}"."fato_conta_receber" ("sk_data_vencimento")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_pagar_sk_categoria" ON "{schema}"."fato_conta_pagar" ("sk_categoria")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_pagar_sk_moeda" ON "{schema}"."fato_conta_pagar" ("sk_moeda")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_pagar_sk_data_pagamento" ON "{schema}"."fato_conta_pagar" ("sk_data_pagamento")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_pagar_sk_data_vencimento" ON "{schema}"."fato_conta_pagar" ("sk_data_vencimento")',
        f'CREATE INDEX IF NOT EXISTS "idx_fato_meta_sk_data_referencia" ON "{schema}"."fato_meta_mensal" ("sk_data_referencia")',
    ]

    with engine.begin() as conn:
        for ddl in index_statements:
            conn.execute(text(ddl))


def reset_star_schema_tables(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f'''TRUNCATE TABLE
                "{schema}"."fato_meta_mensal",
                "{schema}"."fato_conta_pagar",
                "{schema}"."fato_conta_receber",
                "{schema}"."dim_data",
                "{schema}"."dim_moeda",
                "{schema}"."dim_categoria",
                "{schema}"."dim_cliente"
                RESTART IDENTITY CASCADE'''
            )
        )


def drop_star_schema_tables(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f'''DROP TABLE IF EXISTS
                "{schema}"."fato_meta_mensal",
                "{schema}"."fato_conta_pagar",
                "{schema}"."fato_conta_receber",
                "{schema}"."dim_data",
                "{schema}"."dim_moeda",
                "{schema}"."dim_categoria",
                "{schema}"."dim_cliente"
                CASCADE'''
            )
        )


def write_table_controlled(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    mode: str,
    key_columns: Iterable[str] | None = None,
) -> int:
    if df.empty:
        return 0

    if mode not in {"replace", "append"}:
        raise ValueError("Supported postgres write modes: replace, append")

    inspector = inspect(engine)
    exists = inspector.has_table(table_name, schema=schema)

    data_to_write = df.copy()

    if mode == "replace":
        if exists:
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}" CASCADE'))
        data_to_write.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="append" if exists else "fail",
            index=False,
            method="multi",
            chunksize=5000,
        )
        return len(data_to_write)

    if exists and key_columns:
        key_columns = [col for col in key_columns if col in data_to_write.columns]
        if key_columns:
            quoted_cols = ", ".join(f'"{c}"' for c in key_columns)
            existing_keys = pd.read_sql(
                text(f'SELECT {quoted_cols} FROM "{schema}"."{table_name}"'),
                con=engine,
            )
            if not existing_keys.empty:
                existing_keys = existing_keys.drop_duplicates()
                data_to_write = data_to_write.merge(
                    existing_keys,
                    on=key_columns,
                    how="left",
                    indicator=True,
                )
                data_to_write = data_to_write[data_to_write["_merge"] == "left_only"].drop(columns=["_merge"])

    if data_to_write.empty:
        return 0

    data_to_write.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append" if exists else "fail",
        index=False,
        method="multi",
        chunksize=5000,
    )
    return len(data_to_write)
