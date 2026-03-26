from __future__ import annotations

import argparse

from src.config.settings import get_settings
from src.gold.gold_pipeline import run_gold_pipeline
from src.ingestion.bronze_pipeline import run_bronze_ingestion
from src.transform.silver_pipeline import run_silver_pipeline
from src.utils.logging_utils import setup_logging


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Desafio-Ray pipeline runner")
	parser.add_argument(
		"--stage",
		choices=["bronze", "silver", "gold", "all"],
		default="all",
		help="Define which layer to execute",
	)
	parser.add_argument(
		"--table",
		default="all",
		help="Table name for Silver execution (supported: all, metas, categoria, cliente, conta_pagar, conta_receber)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	settings = get_settings()
	logger = setup_logging(settings.logs_path)

	logger.info(
		"Pipeline started",
		extra={"stage": "bootstrap", "dataset": "all", "status": "running"},
	)

	if args.stage in {"bronze", "all"}:
		run_bronze_ingestion(settings=settings, logger=logger)

	if args.stage in {"silver", "all"}:
		run_silver_pipeline(settings=settings, logger=logger, table_name=args.table)

	if args.stage in {"gold", "all"}:
		run_gold_pipeline(settings=settings, logger=logger)

	logger.info(
		"Pipeline finished",
		extra={"stage": "bootstrap", "dataset": "all", "status": "success"},
	)


if __name__ == "__main__":
    main()
