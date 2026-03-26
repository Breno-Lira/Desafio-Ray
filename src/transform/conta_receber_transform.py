from __future__ import annotations

import re
from calendar import isleap

import pandas as pd


ARS_COTACAO_COMPRA_FIXA = 0.0036
ARS_COTACAO_VENDA_FIXA = 0.0037


def _lookup_cotacao_venda(moeda: str, data_pagamento: str, df_bcb: pd.DataFrame) -> float | None:
    if df_bcb is None or df_bcb.empty:
        return None

    if "moeda" not in df_bcb.columns or "data" not in df_bcb.columns or "cotacao_venda" not in df_bcb.columns:
        return None

    match = df_bcb.loc[
        (df_bcb["moeda"] == moeda) & (df_bcb["data"] == data_pagamento),
        "cotacao_venda",
    ]
    if match.empty:
        return None

    value = pd.to_numeric(match.iloc[0], errors="coerce")
    if pd.isna(value):
        return None
    return float(value)


def _calculate_valor_brl(
    valor_conta: float,
    moeda_pais: str | None,
    data_pagamento: str | None,
    df_bcb: pd.DataFrame | None,
) -> float:
    if pd.isna(valor_conta):
        return 0.0

    valor = float(valor_conta)
    moeda = (moeda_pais or "").strip().upper()

    if not moeda or moeda == "BRL":
        return valor

    if not data_pagamento:
        return valor

    # Esses valores de ARS se referem a cotacao do dia 2026-02-26
    # e serao usados para todos os casos/datas com moeda ARS.
    if moeda == "ARS":
        return valor * ARS_COTACAO_VENDA_FIXA

    cotacao_venda = _lookup_cotacao_venda(moeda=moeda, data_pagamento=data_pagamento, df_bcb=df_bcb)
    if cotacao_venda is None:
        return valor

    return valor * cotacao_venda


def transform_conta_receber_table(
    df: pd.DataFrame,
    df_bcb: pd.DataFrame | None = None,
    df_cliente: pd.DataFrame | None = None,
) -> pd.DataFrame:
   
    silver = df.copy()
    silver.columns = [col.strip().lower() for col in silver.columns]

    required_columns = {
        "id_conta",
        "descricao_conta",
        "id_cliente",
        "codigo_categoria",
        "valor_conta",
        "data_vencimento",
        "data_pagamento",
    }
    if not required_columns.issubset(silver.columns):
        raise ValueError(
            f"Expected columns {required_columns}, but got {silver.columns.tolist()}"
        )

    if "status_conta" not in silver.columns:
        silver["status_conta"] = silver["status"] if "status" in silver.columns else None

    if "observacao_conta" not in silver.columns:
        silver["observacao_conta"] = None

    silver["id_conta"] = pd.to_numeric(silver["id_conta"], errors="coerce").astype("Int64")
    silver["id_cliente"] = pd.to_numeric(silver["id_cliente"], errors="coerce").astype("Int64")
    silver = silver[silver["id_conta"].notna() & silver["id_cliente"].notna()].copy()

    silver["valor_conta"] = pd.to_numeric(silver["valor_conta"], errors="coerce")
    silver = silver[silver["valor_conta"].notna()].copy()

    silver["data_vencimento"] = _parse_dates(silver["data_vencimento"])
    silver["data_pagamento"] = _parse_dates(silver["data_pagamento"])

    silver["moeda_pais"] = None
    if df_cliente is not None and not df_cliente.empty:
        cliente = df_cliente.copy()
        cliente.columns = [col.strip().lower() for col in cliente.columns]
        if "id_cliente" in cliente.columns and "moeda_pais" in cliente.columns:
            cliente["id_cliente"] = pd.to_numeric(cliente["id_cliente"], errors="coerce").astype("Int64")
            silver = silver.merge(cliente[["id_cliente", "moeda_pais"]], on="id_cliente", how="left", suffixes=("", "_cliente"))
            if "moeda_pais_cliente" in silver.columns:
                silver["moeda_pais"] = silver["moeda_pais_cliente"]
                silver = silver.drop(columns=["moeda_pais_cliente"])

    silver["valor_conta_brl"] = silver.apply(
        lambda row: _calculate_valor_brl(
            valor_conta=row["valor_conta"],
            moeda_pais=row.get("moeda_pais"),
            data_pagamento=row["data_pagamento"],
            df_bcb=df_bcb,
        ),
        axis=1,
    )

    silver = silver[
        [
            "id_conta",
            "descricao_conta",
            "id_cliente",
            "codigo_categoria",
            "observacao_conta",
            "valor_conta",
            "valor_conta_brl",
            "data_vencimento",
            "data_pagamento",
            "status_conta",
        ]
    ].sort_values(by=["id_conta"]).reset_index(drop=True)

    return silver


def _parse_dates(values: pd.Series) -> pd.Series:
    return values.apply(_parse_date_value)


def _parse_date_value(value: object) -> str | None:
    if pd.isna(value) or value == "":
        return None

    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "nat"):
        return None

    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"):
        try:
            parsed = pd.to_datetime(text, format=date_format)
            return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            corrected = _fix_invalid_feb_29(text=text, date_format=date_format)
            if corrected is not None:
                return corrected
            continue

    return None


def _fix_invalid_feb_29(text: str, date_format: str) -> str | None:
    """Convert invalid 29/02 to 28/02 for non-leap years, preserving output as YYYY-MM-DD."""
    if date_format == "%Y-%m-%d":
        match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
        if not match:
            return None
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    elif date_format == "%d/%m/%Y":
        match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", text)
        if not match:
            return None
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    elif date_format == "%m-%d-%Y":
        match = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", text)
        if not match:
            return None
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    else:
        return None

    if month == 2 and day == 29 and not isleap(year):
        return f"{year:04d}-02-28"

    return None
