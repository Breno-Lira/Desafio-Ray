from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


DAG_ID = "desafio_ray_medallion_pipeline"
SCHEDULE = os.getenv("AIRFLOW_DAG_SCHEDULE", "0 6 * * *")
RETRIES = int(os.getenv("AIRFLOW_DAG_RETRIES", "1"))
RETRY_DELAY_MINUTES = int(os.getenv("AIRFLOW_DAG_RETRY_DELAY_MINUTES", "2"))
TASK_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_TASK_EXECUTION_TIMEOUT_SECONDS", "600"))


def _build_logger():
    from src.config.settings import get_settings
    from src.utils.logging_utils import setup_logging

    settings = get_settings()
    return setup_logging(settings.logs_path)


def _run_bronze() -> None:
    from src.config.settings import get_settings
    from src.ingestion.bronze_pipeline import run_bronze_ingestion

    settings = get_settings()
    logger = _build_logger()
    run_bronze_ingestion(settings=settings, logger=logger)


def _run_silver(table_name: str) -> None:
    from src.config.settings import get_settings
    from src.transform.silver_pipeline import run_silver_pipeline

    settings = get_settings()
    logger = _build_logger()
    run_silver_pipeline(settings=settings, logger=logger, table_name=table_name)


def _run_gold() -> None:
    from src.config.settings import get_settings
    from src.gold.gold_pipeline import run_gold_pipeline

    settings = get_settings()
    logger = _build_logger()
    run_gold_pipeline(settings=settings, logger=logger)


def _run_ml() -> None:
    from src.config.settings import get_settings
    from src.ml.ml_pipeline import run_ml_pipeline

    settings = get_settings()
    logger = _build_logger()
    run_ml_pipeline(settings=settings, logger=logger)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
    "execution_timeout": timedelta(seconds=TASK_TIMEOUT_SECONDS),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Pipeline medallion: bronze -> silver (paralelo) -> gold -> ml",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["desafio-ray", "medallion", "ml"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=_run_bronze,
    )

    silver_meta = PythonOperator(
        task_id="silver_meta",
        python_callable=_run_silver,
        op_kwargs={"table_name": "metas"},
    )
    silver_categoria = PythonOperator(
        task_id="silver_categoria",
        python_callable=_run_silver,
        op_kwargs={"table_name": "categoria"},
    )
    silver_cliente = PythonOperator(
        task_id="silver_cliente",
        python_callable=_run_silver,
        op_kwargs={"table_name": "cliente"},
    )
    silver_conta_pagar = PythonOperator(
        task_id="silver_conta_pagar",
        python_callable=_run_silver,
        op_kwargs={"table_name": "conta_pagar"},
    )
    silver_conta_receber = PythonOperator(
        task_id="silver_conta_receber",
        python_callable=_run_silver,
        op_kwargs={"table_name": "conta_receber"},
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=_run_gold,
    )

    ml = PythonOperator(
        task_id="ml",
        python_callable=_run_ml,
    )

    bronze >> [silver_meta, silver_categoria, silver_cliente, silver_conta_pagar, silver_conta_receber]
    [silver_meta, silver_categoria, silver_cliente, silver_conta_pagar, silver_conta_receber] >> gold >> ml
