from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


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

DAY_NAME_PT = {
    0: "segunda",
    1: "terca",
    2: "quarta",
    3: "quinta",
    4: "sexta",
    5: "sabado",
    6: "domingo",
}


@dataclass
class GoldStarSchema:
    dim_cliente: pd.DataFrame
    dim_categoria: pd.DataFrame
    dim_data: pd.DataFrame
    dim_moeda: pd.DataFrame
    fato_conta_receber: pd.DataFrame
    fato_conta_pagar: pd.DataFrame
    fato_meta_mensal: pd.DataFrame


def build_star_schema(
    df_cliente: pd.DataFrame,
    df_categoria: pd.DataFrame,
    df_meta: pd.DataFrame,
    df_conta_pagar: pd.DataFrame,
    df_conta_receber: pd.DataFrame,
) -> GoldStarSchema:
    dim_cliente = _build_dim_cliente(df_cliente)
    dim_categoria = _build_dim_categoria(df_categoria)
    dim_moeda = _build_dim_moeda(df_cliente=df_cliente)
    dim_data = _build_dim_data(
        df_meta=df_meta,
        df_conta_pagar=df_conta_pagar,
        df_conta_receber=df_conta_receber,
    )

    fato_conta_receber = _build_fato_conta_receber(
        df_conta_receber=df_conta_receber,
        dim_cliente=dim_cliente,
        dim_categoria=dim_categoria,
        dim_moeda=dim_moeda,
        dim_data=dim_data,
    )
    fato_conta_pagar = _build_fato_conta_pagar(
        df_conta_pagar=df_conta_pagar,
        dim_categoria=dim_categoria,
        dim_moeda=dim_moeda,
        dim_data=dim_data,
    )
    fato_meta_mensal = _build_fato_meta_mensal(
        df_meta=df_meta,
        dim_data=dim_data,
    )

    return GoldStarSchema(
        dim_cliente=dim_cliente,
        dim_categoria=dim_categoria,
        dim_data=dim_data,
        dim_moeda=dim_moeda,
        fato_conta_receber=fato_conta_receber,
        fato_conta_pagar=fato_conta_pagar,
        fato_meta_mensal=fato_meta_mensal,
    )


def _build_dim_cliente(df_cliente: pd.DataFrame) -> pd.DataFrame:
    dim = df_cliente.copy()
    dim.columns = [c.strip().lower() for c in dim.columns]

    dim["id_cliente"] = pd.to_numeric(dim["id_cliente"], errors="coerce").astype("Int64")
    dim = dim[dim["id_cliente"].notna()].copy()
    dim = dim.drop_duplicates(subset=["id_cliente"]).sort_values("id_cliente").reset_index(drop=True)

    return dim[
        [
            "id_cliente",
            "nome_cliente",
            "pais_cliente",
            "moeda_pais",
            "data_cadastro_cliente",
        ]
    ]


def _build_dim_categoria(df_categoria: pd.DataFrame) -> pd.DataFrame:
    dim = df_categoria.copy()
    dim.columns = [c.strip().lower() for c in dim.columns]

    dim["codigo_categoria"] = dim["codigo_categoria"].astype(str).str.strip()
    dim = dim[dim["codigo_categoria"].ne("")].copy()
    dim = dim.drop_duplicates(subset=["codigo_categoria"]).sort_values("codigo_categoria").reset_index(drop=True)

    dim.insert(0, "sk_categoria", range(1, len(dim) + 1))

    return dim[["sk_categoria", "codigo_categoria", "nome_categoria", "tipo_categoria"]]


def _build_dim_moeda(df_cliente: pd.DataFrame) -> pd.DataFrame:
    moedas = set(df_cliente["moeda_pais"].dropna().astype(str).str.upper().str.strip().tolist())
    moedas.add("BRL")

    dim = pd.DataFrame({"codigo_moeda": sorted(moedas)})
    dim.insert(0, "sk_moeda", range(1, len(dim) + 1))
    return dim[["sk_moeda", "codigo_moeda"]]


def _build_dim_data(df_meta: pd.DataFrame, df_conta_pagar: pd.DataFrame, df_conta_receber: pd.DataFrame) -> pd.DataFrame:
    datas = []

    for col in ["mes_ano_meta"]:
        if col in df_meta.columns:
            datas.extend(pd.to_datetime(df_meta[col], errors="coerce").dropna().dt.date.tolist())

    for col in ["data_vencimento", "data_pagamento"]:
        if col in df_conta_pagar.columns:
            datas.extend(pd.to_datetime(df_conta_pagar[col], errors="coerce").dropna().dt.date.tolist())

    for col in ["data_vencimento", "data_pagamento"]:
        if col in df_conta_receber.columns:
            datas.extend(pd.to_datetime(df_conta_receber[col], errors="coerce").dropna().dt.date.tolist())

    dim = pd.DataFrame({"data_calendario": sorted(set(datas))})
    if dim.empty:
        return pd.DataFrame(
            columns=[
                "sk_data",
                "data_calendario",
                "ano",
                "trimestre",
                "mes",
                "nome_mes",
                "dia",
                "dia_semana_num",
                "nome_dia_semana",
                "fim_de_semana",
            ]
        )

    dim["data_calendario"] = pd.to_datetime(dim["data_calendario"], errors="coerce")
    dim["ano"] = dim["data_calendario"].dt.year.astype("Int64")
    dim["trimestre"] = dim["data_calendario"].dt.quarter.astype("Int64")
    dim["mes"] = dim["data_calendario"].dt.month.astype("Int64")
    dim["nome_mes"] = dim["mes"].map(MONTH_NAME_PT)
    dim["dia"] = dim["data_calendario"].dt.day.astype("Int64")
    dim["dia_semana_num"] = dim["data_calendario"].dt.weekday.astype("Int64")
    dim["nome_dia_semana"] = dim["dia_semana_num"].map(DAY_NAME_PT)
    dim["fim_de_semana"] = dim["dia_semana_num"].isin([5, 6])

    dim.insert(0, "sk_data", range(1, len(dim) + 1))

    dim["data_calendario"] = dim["data_calendario"].dt.date

    return dim[
        [
            "sk_data",
            "data_calendario",
            "ano",
            "trimestre",
            "mes",
            "nome_mes",
            "dia",
            "dia_semana_num",
            "nome_dia_semana",
            "fim_de_semana",
        ]
    ]


def _build_fato_conta_receber(
    df_conta_receber: pd.DataFrame,
    dim_cliente: pd.DataFrame,
    dim_categoria: pd.DataFrame,
    dim_moeda: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df_conta_receber.copy()
    fato.columns = [c.strip().lower() for c in fato.columns]

    fato["id_conta"] = pd.to_numeric(fato["id_conta"], errors="coerce").astype("Int64")
    fato = fato[fato["id_conta"].notna()].copy()
    fato = fato.drop_duplicates(subset=["id_conta"], keep="last").copy()

    cliente_map = dim_cliente[["id_cliente", "moeda_pais"]].rename(columns={"moeda_pais": "codigo_moeda"})
    categoria_map = dim_categoria[["codigo_categoria", "sk_categoria"]]
    moeda_map = dim_moeda[["codigo_moeda", "sk_moeda"]]
    data_map = dim_data[["data_calendario", "sk_data"]]

    fato["id_cliente"] = pd.to_numeric(fato["id_cliente"], errors="coerce").astype("Int64")
    fato["codigo_categoria"] = fato["codigo_categoria"].astype(str).str.strip()

    fato = fato.merge(cliente_map, on="id_cliente", how="left")
    fato = fato.merge(categoria_map, on="codigo_categoria", how="left")
    fato["codigo_moeda"] = fato["codigo_moeda"].fillna("BRL").astype(str).str.upper()
    fato = fato.merge(moeda_map, on="codigo_moeda", how="left")

    fato["data_pagamento_dt"] = pd.to_datetime(fato["data_pagamento"], errors="coerce").dt.date
    fato["data_vencimento_dt"] = pd.to_datetime(fato["data_vencimento"], errors="coerce").dt.date

    fato = fato.merge(data_map.rename(columns={"data_calendario": "data_pagamento_dt", "sk_data": "sk_data_pagamento"}), on="data_pagamento_dt", how="left")
    fato = fato.merge(data_map.rename(columns={"data_calendario": "data_vencimento_dt", "sk_data": "sk_data_vencimento"}), on="data_vencimento_dt", how="left")

    return fato[
        [
            "id_conta",
            "id_cliente",
            "sk_categoria",
            "sk_moeda",
            "sk_data_pagamento",
            "sk_data_vencimento",
            "valor_conta",
            "valor_conta_brl",
            "status_conta",
            "observacao_conta",
        ]
    ].rename(columns={"id_conta": "id_conta_receber"})


def _build_fato_conta_pagar(
    df_conta_pagar: pd.DataFrame,
    dim_categoria: pd.DataFrame,
    dim_moeda: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df_conta_pagar.copy()
    fato.columns = [c.strip().lower() for c in fato.columns]

    fato["id_conta"] = pd.to_numeric(fato["id_conta"], errors="coerce").astype("Int64")
    fato = fato[fato["id_conta"].notna()].copy()
    fato = fato.drop_duplicates(subset=["id_conta"], keep="last").copy()

    categoria_map = dim_categoria[["codigo_categoria", "sk_categoria"]]
    moeda_map = dim_moeda[["codigo_moeda", "sk_moeda"]]
    data_map = dim_data[["data_calendario", "sk_data"]]

    fato["codigo_categoria"] = fato["codigo_categoria"].astype(str).str.strip()
    fato = fato.merge(categoria_map, on="codigo_categoria", how="left")
    fato["codigo_moeda"] = "BRL"
    fato = fato.merge(moeda_map, on="codigo_moeda", how="left")

    fato["data_pagamento_dt"] = pd.to_datetime(fato["data_pagamento"], errors="coerce").dt.date
    fato["data_vencimento_dt"] = pd.to_datetime(fato["data_vencimento"], errors="coerce").dt.date

    fato = fato.merge(data_map.rename(columns={"data_calendario": "data_pagamento_dt", "sk_data": "sk_data_pagamento"}), on="data_pagamento_dt", how="left")
    fato = fato.merge(data_map.rename(columns={"data_calendario": "data_vencimento_dt", "sk_data": "sk_data_vencimento"}), on="data_vencimento_dt", how="left")

    return fato[
        [
            "id_conta",
            "sk_categoria",
            "sk_moeda",
            "sk_data_pagamento",
            "sk_data_vencimento",
            "valor_conta",
            "valor_conta_brl",
            "status_conta",
            "destino_pagamento_conta",
            "observacao_conta",
        ]
    ].rename(columns={"id_conta": "id_conta_pagar"})


def _build_fato_meta_mensal(df_meta: pd.DataFrame, dim_data: pd.DataFrame) -> pd.DataFrame:
    fato = df_meta.copy()
    fato.columns = [c.strip().lower() for c in fato.columns]

    data_map = dim_data[["data_calendario", "sk_data"]]

    fato["mes_ano_meta_dt"] = pd.to_datetime(fato["mes_ano_meta"], errors="coerce").dt.date
    fato = fato.merge(
        data_map.rename(columns={"data_calendario": "mes_ano_meta_dt", "sk_data": "sk_data_referencia"}),
        on="mes_ano_meta_dt",
        how="left",
    )

    fato.insert(0, "sk_fato_meta", range(1, len(fato) + 1))

    return fato[["sk_fato_meta", "sk_data_referencia", "meta_valor", "mes", "ano", "nome_mes"]]
