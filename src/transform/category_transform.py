from __future__ import annotations

import re
import unicodedata

import pandas as pd



def transform_category_table(df: pd.DataFrame) -> pd.DataFrame:
    silver = df.copy()
    silver.columns = [col.strip().lower() for col in silver.columns]

    required_columns = {"codigo_categoria", "nome_categoria", "tipo_categoria"}
    if not required_columns.issubset(silver.columns):
        raise ValueError("Expected columns codigo_categoria, nome_categoria and tipo_categoria")

    silver["codigo_categoria"] = silver["codigo_categoria"].apply(_normalize_key)
    silver["nome_categoria"] = silver["nome_categoria"].apply(_normalize_text)
    silver["tipo_categoria"] = silver["tipo_categoria"].apply(_normalize_text)

    # Enforce key integrity by removing rows without category id.
    silver = silver[silver["codigo_categoria"].notna()].copy()

    silver["_has_nome"] = silver["nome_categoria"].notna()
    silver["_has_tipo"] = silver["tipo_categoria"].notna()
    silver["_original_order"] = range(len(silver))

    # Keep one row per id: first prefer non-null name, then non-null type.
    silver = silver.sort_values(
        by=["codigo_categoria", "_has_nome", "_has_tipo", "_original_order"],
        ascending=[True, False, False, True],
    )
    silver = silver.drop_duplicates(subset=["codigo_categoria"], keep="first")

    silver = silver[["codigo_categoria", "nome_categoria", "tipo_categoria"]]
    silver = silver.sort_values(by=["codigo_categoria"]).reset_index(drop=True)
    return silver



def _normalize_key(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None



def _normalize_text(value: object) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    text = _remove_accents(text)
    text = text.upper()
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None



def _remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))
