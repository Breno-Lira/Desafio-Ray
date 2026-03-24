from src.config.settings import get_settings
from src.ingestion.bronze_pipeline import run_bronze_ingestion
from src.utils.logging_utils import setup_logging


def main() -> None:
	settings = get_settings()
	logger = setup_logging(settings.logs_path)

	logger.info(
		"Pipeline started",
		extra={"stage": "bootstrap", "dataset": "all", "status": "running"},
	)

	run_bronze_ingestion(settings=settings, logger=logger)

	logger.info(
		"Pipeline finished",
		extra={"stage": "bootstrap", "dataset": "all", "status": "success"},
	)


if __name__ == "__main__":
	main()
