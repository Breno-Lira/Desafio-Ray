from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.compose import ColumnTransformer
from sklearn.metrics import calinski_harabasz_score, davies_bouldin_score, silhouette_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config.settings import Settings
from src.ingestion.file_loader import write_parquet


def _log(logger: logging.Logger, level: int, message: str, **kwargs: object) -> None:
    logger.log(level, message, extra=kwargs)


def run_ml_pipeline(settings: Settings, logger: logging.Logger) -> None:
    silver = _load_silver_inputs(settings)
    features_df = _build_customer_features(silver)

    if features_df.empty:
        _log(
            logger,
            logging.WARNING,
            "ML stage skipped: no customer features available",
            stage="ml",
            dataset="features_cliente",
            status="skipped",
        )
        return

    model_comparison_df, clustered_df = _fit_and_select_clustering_model(features_df)

    write_parquet(features_df, settings.data_ml_path / "features_cliente", settings.ml_format)
    write_parquet(model_comparison_df, settings.data_ml_path / "model_comparison", settings.ml_format)
    write_parquet(clustered_df, settings.data_ml_path / "cliente_clusterizado", settings.ml_format)

    winner = model_comparison_df.sort_values(by="ranking_score", ascending=False).iloc[0]

    _log(
        logger,
        logging.INFO,
        "ML clustering completed",
        stage="ml",
        dataset="cliente_clusterizado",
        status="success",
    )
    _log(
        logger,
        logging.INFO,
        (
            f"Clientes={len(clustered_df)} "
            f"ModeloSelecionado={winner['model_name']} "
            f"Silhouette={winner['silhouette']:.4f} "
            f"DaviesBouldin={winner['davies_bouldin']:.4f} "
            f"CalinskiHarabasz={winner['calinski_harabasz']:.4f}"
        ),
        stage="ml",
        dataset="model_comparison",
        status="metadata",
    )


def _load_silver_inputs(settings: Settings) -> dict[str, pd.DataFrame]:
    return {
        "cliente": _read_dataset(settings.data_silver_path, "cliente_silver"),
        "conta_receber": _read_dataset(settings.data_silver_path, "conta_receber_silver"),
        "categoria": _read_dataset(settings.data_silver_path, "categoria_silver"),
    }


def _read_dataset(base_path: Path, base_name: str) -> pd.DataFrame:
    parquet_path = base_path / f"{base_name}.parquet"
    csv_path = base_path / f"{base_name}.csv"

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"Silver dataset not found: {base_name}.parquet/csv")


def _build_customer_features(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    cliente = silver["cliente"].copy()
    receber = silver["conta_receber"].copy()
    categoria = silver["categoria"].copy()

    cliente.columns = [c.strip().lower() for c in cliente.columns]
    receber.columns = [c.strip().lower() for c in receber.columns]
    categoria.columns = [c.strip().lower() for c in categoria.columns]

    cliente["id_cliente"] = pd.to_numeric(cliente["id_cliente"], errors="coerce").astype("Int64")
    cliente = cliente[cliente["id_cliente"].notna()].copy()

    receber["id_cliente"] = pd.to_numeric(receber["id_cliente"], errors="coerce").astype("Int64")
    receber["valor_conta_brl"] = pd.to_numeric(receber["valor_conta_brl"], errors="coerce")
    receber["data_pagamento"] = pd.to_datetime(receber["data_pagamento"], errors="coerce")
    receber["data_vencimento"] = pd.to_datetime(receber["data_vencimento"], errors="coerce")

    ref_date = receber["data_pagamento"].max()
    if pd.isna(ref_date):
        ref_date = pd.Timestamp.utcnow().normalize()

    base = cliente[["id_cliente", "nome_cliente", "pais_cliente", "moeda_pais", "data_cadastro_cliente"]].copy()

    agg = (
        receber.groupby("id_cliente", dropna=True)
        .agg(
            frequencia_recebimentos=("id_conta", "count"),
            valor_total_recebido_brl=("valor_conta_brl", "sum"),
            valor_medio_recebido_brl=("valor_conta_brl", "mean"),
            ultimo_pagamento=("data_pagamento", "max"),
            status_mais_comum=("status_conta", lambda s: _mode_or_default(s, "sem_status")),
        )
        .reset_index()
    )

    agg["recencia_dias"] = (ref_date - agg["ultimo_pagamento"]).dt.days
    agg["recencia_dias"] = agg["recencia_dias"].fillna(9999).astype(float)

    delay = receber.copy()
    delay["dias_atraso"] = (delay["data_pagamento"] - delay["data_vencimento"]).dt.days
    delay["dias_atraso"] = delay["dias_atraso"].fillna(0)
    delay["pago_em_dia"] = (delay["dias_atraso"] <= 0).astype(int)
    delay["pago_com_atraso"] = (delay["dias_atraso"] > 0).astype(int)

    behavior = (
        delay.groupby("id_cliente", dropna=True)
        .agg(
            media_dias_atraso=("dias_atraso", "mean"),
            taxa_pagamento_em_dia=("pago_em_dia", "mean"),
            taxa_pagamento_atrasado=("pago_com_atraso", "mean"),
        )
        .reset_index()
    )

    cat = receber[["id_cliente", "codigo_categoria"]].copy()
    if not categoria.empty and {"codigo_categoria", "nome_categoria"}.issubset(categoria.columns):
        cat = cat.merge(
            categoria[["codigo_categoria", "nome_categoria"]],
            on="codigo_categoria",
            how="left",
        )
    else:
        cat["nome_categoria"] = cat["codigo_categoria"]

    cat["nome_categoria"] = cat["nome_categoria"].astype(str).str.strip().replace({"": "SEM_CATEGORIA"})

    mix = (
        cat.groupby(["id_cliente", "nome_categoria"], dropna=True)
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    category_cols = [c for c in mix.columns if c != "id_cliente"]
    if category_cols:
        totals = mix[category_cols].sum(axis=1).replace(0, 1)
        mix[category_cols] = mix[category_cols].div(totals, axis=0)
        mix = mix.rename(columns={c: f"mix_cat_{_safe_col(c)}" for c in category_cols})

    features = base.merge(agg, on="id_cliente", how="left")
    features = features.merge(behavior, on="id_cliente", how="left")
    features = features.merge(mix, on="id_cliente", how="left")

    numeric_cols = features.select_dtypes(include=["number"]).columns.tolist()
    for col in numeric_cols:
        if col == "id_cliente":
            continue
        features[col] = features[col].fillna(0)

    # Cria uma faixa categórica de valor para enriquecer o treino do clustering.
    features["faixa_valor_cliente"] = _build_value_band(
        features["valor_total_recebido_brl"],
    )

    features["status_mais_comum"] = features["status_mais_comum"].fillna("sem_status")
    features["pais_cliente"] = features["pais_cliente"].fillna("DESCONHECIDO")
    features["moeda_pais"] = features["moeda_pais"].fillna("DESCONHECIDA")

    return features


def _fit_and_select_clustering_model(features_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = features_df.copy()

    feature_cols = [
        c
        for c in data.columns
        if c
        not in {
            "id_cliente",
            "nome_cliente",
            "data_cadastro_cliente",
        }
    ]

    x = data[feature_cols].copy()
    numeric_cols = x.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = [c for c in x.columns if c not in numeric_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
        ]
    )

    x_scaled = preprocessor.fit_transform(x)

    candidate_results: list[dict[str, object]] = []

    candidate_results.extend(_evaluate_kmeans_candidates(x_scaled))
    candidate_results.extend(_evaluate_agglomerative_candidates(x_scaled))

    if not candidate_results:
        fallback = KMeans(n_clusters=2, n_init=20, random_state=42)
        labels = fallback.fit_predict(x_scaled)
        candidate_results.append(_metrics_row("kmeans_k2_fallback", labels, x_scaled))

    comparison = pd.DataFrame(candidate_results)
    comparison = _rank_models(comparison)

    best = comparison.sort_values(by="ranking_score", ascending=False).iloc[0]

    best_labels = _fit_labels_from_name(model_name=str(best["model_name"]), x_scaled=x_scaled)

    clustered = data[["id_cliente", "nome_cliente", "pais_cliente", "moeda_pais"]].copy()
    clustered["cluster"] = best_labels
    clustered["cluster"] = clustered["cluster"].astype(int)

    clustered = clustered.merge(
        data[[
            "id_cliente",
            "recencia_dias",
            "frequencia_recebimentos",
            "valor_total_recebido_brl",
            "faixa_valor_cliente",
            "taxa_pagamento_em_dia",
            "taxa_pagamento_atrasado",
        ]],
        on="id_cliente",
        how="left",
    )

    clustered["perfil_cluster"] = clustered.apply(_build_profile_label, axis=1)
    clustered["modelo_vencedor"] = best["model_name"]

    return comparison, clustered


def _evaluate_kmeans_candidates(x_scaled: np.ndarray) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for k in range(2, 9):
        model = KMeans(n_clusters=k, n_init=20, random_state=42)
        labels = model.fit_predict(x_scaled)
        rows.append(_metrics_row(f"kmeans_k{k}", labels, x_scaled))
    return rows


def _evaluate_agglomerative_candidates(x_scaled: np.ndarray) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for k in range(2, 9):
        model = AgglomerativeClustering(n_clusters=k)
        labels = model.fit_predict(x_scaled)
        rows.append(_metrics_row(f"agglomerative_k{k}", labels, x_scaled))
    return rows





def _metrics_row(model_name: str, labels: np.ndarray, x_scaled: np.ndarray) -> dict[str, object]:
    valid = labels != -1
    unique_clusters = np.unique(labels[valid]) if valid.any() else np.array([])

    if len(unique_clusters) < 2:
        return {
            "model_name": model_name,
            "n_clusters": int(len(unique_clusters)),
            "noise_ratio": float((labels == -1).mean()),
            "silhouette": -1.0,
            "davies_bouldin": 999999.0,
            "calinski_harabasz": 0.0,
        }

    x_eval = x_scaled[valid]
    y_eval = labels[valid]

    return {
        "model_name": model_name,
        "n_clusters": int(len(unique_clusters)),
        "noise_ratio": float((labels == -1).mean()),
        "silhouette": float(silhouette_score(x_eval, y_eval)),
        "davies_bouldin": float(davies_bouldin_score(x_eval, y_eval)),
        "calinski_harabasz": float(calinski_harabasz_score(x_eval, y_eval)),
    }


def _rank_models(comparison: pd.DataFrame) -> pd.DataFrame:
    ranked = comparison.copy()

    ranked["rank_silhouette"] = ranked["silhouette"].rank(ascending=False, method="average")
    ranked["rank_davies_bouldin"] = ranked["davies_bouldin"].rank(ascending=True, method="average")
    ranked["rank_calinski_harabasz"] = ranked["calinski_harabasz"].rank(ascending=False, method="average")

    ranked["ranking_score"] = 1.0 / (
        ranked["rank_silhouette"]
        + ranked["rank_davies_bouldin"]
        + ranked["rank_calinski_harabasz"]
    )

    return ranked.sort_values(by="ranking_score", ascending=False).reset_index(drop=True)


def _fit_labels_from_name(model_name: str, x_scaled: np.ndarray) -> np.ndarray:
    if model_name.startswith("kmeans_k"):
        k = int(model_name.replace("kmeans_k", ""))
        return KMeans(n_clusters=k, n_init=20, random_state=42).fit_predict(x_scaled)

    if model_name.startswith("agglomerative_k"):
        k = int(model_name.replace("agglomerative_k", ""))
        return AgglomerativeClustering(n_clusters=k).fit_predict(x_scaled)

    

    return KMeans(n_clusters=2, n_init=20, random_state=42).fit_predict(x_scaled)


def _build_profile_label(row: pd.Series) -> str:
    recencia = float(row.get("recencia_dias", 0.0))
    frequencia = float(row.get("frequencia_recebimentos", 0.0))
    valor = float(row.get("valor_total_recebido_brl", 0.0))
    taxa_em_dia = float(row.get("taxa_pagamento_em_dia", 0.0))

    rec = "recente" if recencia <= 60 else "inativo"
    freq = "alta_frequencia" if frequencia >= 20 else "baixa_frequencia"
    faixa_valor = str(row.get("faixa_valor_cliente", "")).strip().lower()
    if faixa_valor in {"muito_alto_valor", "alto_valor", "medio_baixo_valor"}:
        mon = faixa_valor
    else:
        mon = "alto_valor" if valor >= 100000 else "medio_baixo_valor"
    pay = "bom_pagador" if taxa_em_dia >= 0.7 else "atrasa_pagamento"

    return f"{rec}|{freq}|{mon}|{pay}"


def _build_value_band(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0)
    rank_pct = numeric.rank(method="average", pct=True)

    return pd.Series(
        np.select(
            [rank_pct > (2 / 3), rank_pct > (1 / 3)],
            ["muito_alto_valor", "alto_valor"],
            default="medio_baixo_valor",
        ),
        index=series.index,
        dtype="object",
    )


def _mode_or_default(series: pd.Series, default: str) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    if cleaned.empty:
        return default
    mode = cleaned.mode()
    if mode.empty:
        return default
    return str(mode.iloc[0])


def _safe_col(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in value.lower())
    safe = "_".join(part for part in safe.split("_") if part)
    return safe or "sem_categoria"
