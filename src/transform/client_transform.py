from __future__ import annotations

import re
import unicodedata
from datetime import datetime

import pandas as pd

COUNTRY_CANONICAL_MAP = {
    "BRASIL": "BRASIL",
    "PORTUGAL": "PORTUGAL",
    "FRANCA": "FRANCA",
    "ESTADOS UNIDOS": "ESTADOS UNIDOS",
    "EUA": "ESTADOS UNIDOS",
    "USA": "ESTADOS UNIDOS",
    "ESTADOS UNIDOS DA AMERICA": "ESTADOS UNIDOS",
    "CANADA": "CANADA",
    "ARGENTINA": "ARGENTINA",
    "INGLATERRA": "REINO UNIDO",
    "REINO UNIDO": "REINO UNIDO",
    "UK": "REINO UNIDO",
}

CURRENCY_BY_COUNTRY = {
    "BRASIL": "BRL",
    "PORTUGAL": "EUR",
    "FRANCA": "EUR",
    "ESTADOS UNIDOS": "USD",
    "CANADA": "CAD",
    "ARGENTINA": "ARS",
    "REINO UNIDO": "GBP",
}



def transform_client_table(df: pd.DataFrame) -> pd.DataFrame:
    silver = df.copy()
    silver.columns = [col.strip().lower() for col in silver.columns]

    required_columns = {
        "id_cliente",
        "nome_cliente",
        "pais_cliente",
        "data_cadastro_cliente",
    }
    if not required_columns.issubset(silver.columns):
        raise ValueError(
            "Expected columns id_cliente, nome_cliente, pais_cliente and data_cadastro_cliente"
        )

    silver["id_cliente"] = pd.to_numeric(silver["id_cliente"], errors="coerce").astype("Int64")
    silver = silver[silver["id_cliente"].notna()].copy()

    silver["nome_cliente"] = silver["nome_cliente"].apply(_normalize_person_name)
    silver["pais_cliente"] = silver["pais_cliente"].apply(_normalize_country)

    parsed_dates = _parse_client_dates(silver["data_cadastro_cliente"])
    silver["data_cadastro_cliente"] = pd.to_datetime(parsed_dates)
    silver["data_cadastro_cliente"] = silver["data_cadastro_cliente"].dt.date


    silver["moeda_pais"] = silver["pais_cliente"].map(CURRENCY_BY_COUNTRY)

    silver = silver[
        [
            "id_cliente",
            "nome_cliente",
            "pais_cliente",
            "moeda_pais",
            "data_cadastro_cliente",
        ]
    ].sort_values(by=["id_cliente"]).reset_index(drop=True)

    return silver



def _parse_client_dates(values: pd.Series) -> pd.Series:
    return values.apply(_parse_date_value)


def _parse_date_value(value: object) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    text = str(value).strip()
    if not text:
        return pd.NaT

    for date_format in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(text, date_format)
            return pd.Timestamp(parsed)
        except ValueError:
            continue

    return pd.NaT



def _normalize_person_name(value: object) -> str | None:
    if pd.isna(value):
        return None

    text = _fix_mojibake(str(value).strip())
    text = re.sub(r"\s+", " ", text)

    if not text:
        return None

    return " ".join(_smart_title(token) for token in text.split(" "))



def _normalize_country(value: object) -> str | None:
    if pd.isna(value):
        return None

    text = _fix_mojibake(str(value).strip())
    text = re.sub(r"\s+", " ", text)
    if not text:
        return None

    normalized_key = _remove_accents(text).upper()
    normalized_key = re.sub(r"\s+", " ", normalized_key).strip()

    return COUNTRY_CANONICAL_MAP.get(normalized_key, normalized_key)



def _smart_title(token: str) -> str:
    if not token:
        return token

    lowered = token.lower()
    if lowered in {"da", "de", "do", "das", "dos", "e"}:
        return lowered

    return lowered[0].upper() + lowered[1:]



def _fix_mojibake(text: str) -> str:
    # Fixes common UTF-8 interpreted as Latin-1 issues, e.g., 'FranÃ§a'.
    if "Ã" not in text and "Â" not in text and "�" not in text:
        return text

    try:
        fixed = text.encode("latin1").decode("utf-8")
        return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text



def _remove_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))
