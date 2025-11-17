"""
 Copyright Duel 2025
"""

import argparse
import logging
from pathlib import Path

from src.pipeline.advocate_ingester import AdvocateIngester
logger = logging.getLogger(__name__)

class AdvocatePipeline:
    """
    Manages the advocate ingestion pipeline.
    """

    @staticmethod
    def _configure_logging(verbosity: int) -> None:
        """
        Configures the logging system for the application based on the provided verbosity level.
        This method determines the log level and sets up the logging format accordingly.

        :param verbosity: An integer representing the verbosity level for logging.
        """
        level = logging.WARNING
        if verbosity == 1:
            level = logging.INFO
        elif verbosity >= 2:
            level = logging.DEBUG

        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )

    @staticmethod
    def _parse_args() -> argparse.Namespace:
        """
        Parses command-line arguments for the advocate ingestion pipeline.
        :return: Parsed command-line arguments
        """
        parser = argparse.ArgumentParser(
            description="Run the advocate ingestion pipeline.",
        )
        parser.add_argument(
            "--ingest-dir",
            type=Path,
            required=True,
            help="Directory containing raw advocate JSON files (default: ./raw_data)",
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            required=False,
            default=8,
            help="Maximum number of worker threads for ingestion (default: 8)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Only validate and collect stats without persisting records to MongoDB.",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            required=False,
            default=0,
            help="Increase log verbosity (use -vv for debug).",
        )
        return parser.parse_args()


    def run(self) -> None:
        """
        Parses command-line arguments, configures logging, and executes the AdvocateIngester to process
        files in the specified directory. Outputs a summary of the ingestion process upon completion.
        """
        # Parse command line arguments
        args = AdvocatePipeline._parse_args()
        AdvocatePipeline._configure_logging(args.verbose)
        ingest_dir: Path = args.ingest_dir
        max_workers: int = args.max_workers
        dry_run : bool = args.dry_run

        # Instantiate and run advocate ingester
        ingester = AdvocateIngester(ingest_dir=ingest_dir, max_workers=max_workers, write_to_datastore=not dry_run)
        stats = ingester.run()

        # Simple summary output
        logger.info(
            "Ingest complete: files_seen=%d, files_parsed=%d, files_skipped=%d, "
            "records_valid=%d, records_invalid=%d",
            stats.files_seen,
            stats.files_parsed,
            stats.files_skipped,
            stats.records_valid,
            stats.records_invalid,
        )

def main():
    AdvocatePipeline().run()
