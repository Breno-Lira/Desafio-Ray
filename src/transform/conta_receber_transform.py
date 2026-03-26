from __future__ import annotations

import pandas as pd


def transform_conta_receber_table(df: pd.DataFrame, df_bcb: pd.DataFrame | None = None) -> pd.DataFrame:
   
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

    silver["valor_conta_brl"] = silver["valor_conta"]

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
    """Parse dates to DD/MM/YYYY format."""
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
            return parsed.strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            continue

    return None
