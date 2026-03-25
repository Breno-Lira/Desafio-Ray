from __future__ import annotations

import re

import pandas as pd

MONTH_MAP = {
    "01": 1,
    "02": 2,
    "03": 3,
    "04": 4,
    "05": 5,
    "06": 6,
    "07": 7,
    "08": 8,
    "09": 9,
    "10": 10,
    "11": 11,
    "12": 12,
    "jan": 1,
    "janeiro": 1,
    "fev": 2,
    "fevereiro": 2,
    "mar": 3,
    "marco": 3,
    "abril": 4,
    "abr": 4,
    "mai": 5,
    "maio": 5,
    "jun": 6,
    "junho": 6,
    "jul": 7,
    "julho": 7,
    "ago": 8,
    "agosto": 8,
    "set": 9,
    "setembro": 9,
    "out": 10,
    "outubro": 10,
    "nov": 11,
    "novembro": 11,
    "dez": 12,
    "dezembro": 12,
}

MONTH_NAME_PT = {
    1: "janeiro",
    2: "fevereiro",
    3: "marco",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro",
}


def transform_meta_table(df: pd.DataFrame) -> pd.DataFrame:
    silver = df.copy()

    silver.columns = [col.strip().lower() for col in silver.columns]

    if "mes_meta" not in silver.columns or "meta" not in silver.columns:
        raise ValueError("Expected columns 'mes_meta' and 'meta' in metas dataset")

    silver["mes"] = silver["mes_meta"].apply(_parse_month)
    silver["ano"] = silver["mes_meta"].apply(_parse_year)
    silver["mes_ano_meta"] = silver.apply(_build_month_reference, axis=1)
    silver["meta_valor"] = silver["meta"].apply(_parse_meta_value)
    silver["nome_mes"] = silver["mes"].apply(_build_month_name)

    silver["mes"] = pd.to_numeric(silver["mes"], errors="coerce").astype("Int64")
    silver["ano"] = pd.to_numeric(silver["ano"], errors="coerce").astype("Int64")

    silver["meta_valor"] = silver["meta_valor"].astype("Float64")

    ordered_columns = [
        "mes_ano_meta",
        "meta_valor",
        "mes",
        "ano",
        "nome_mes",
    ]
    return silver[ordered_columns]


def _normalize_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.strip().lower()
    replacements = {
        "ç": "c",
        "á": "a",
        "à": "a",
        "â": "a",
        "ã": "a",
        "é": "e",
        "ê": "e",
        "í": "i",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ú": "u",
        ".": "",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return re.sub(r"\s+", " ", text)


def _parse_month(value: object) -> int | None:
    text = _normalize_text(value)

    direct_match = re.search(r"^(\d{2})/(\d{4})$", text)
    if direct_match:
        return int(direct_match.group(1))

    month_word_match = re.search(r"([a-z]+)", text)
    if month_word_match:
        month_token = month_word_match.group(1)
        return MONTH_MAP.get(month_token)

    return None


def _parse_year(value: object) -> int | None:
    text = _normalize_text(value)
    year_match = re.search(r"(\d{4})", text)
    if not year_match:
        return None
    return int(year_match.group(1))


def _build_month_reference(row: pd.Series) -> str | None:
    month = row.get("mes")
    year = row.get("ano")
    if pd.isna(month) or pd.isna(year):
        return None
    return f"{int(month):02d}-{int(year):04d}"


def _build_month_name(month: object) -> str | None:
    if pd.isna(month):
        return None
    return MONTH_NAME_PT.get(int(month))


def _parse_meta_value(value: object) -> float | None:
    if pd.isna(value):
        return None

    text = str(value).strip().lower().replace(".", "").replace(",", ".")

    if text.endswith("k"):
        numeric = text[:-1]
        try:
            return float(numeric) * 1000
        except ValueError:
            return None

    try:
        return float(text)
    except ValueError:
        return None
