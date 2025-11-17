"""
 Copyright Dual 2025
"""

import argparse
import logging
from pathlib import Path

from src.pipeline.advocate_ingester import AdvocateIngester


class AdvocatePipeline:

    @staticmethod
    def _configure_logging(verbosity: int) -> None:
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
            type=bool,
            help="Only validate and collect stats without persisting records to MongoDB",
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
        print(
            f"Ingest finished. "
            f"files_seen={stats.files_seen}, "
            f"files_parsed={stats.files_parsed}, "
            f"files_skipped={stats.files_skipped}, "
            f"records_valid={stats.records_valid}, "
            f"records_invalid={stats.records_invalid}",
        )
       # print(f"Total advocates loaded: {len(stats.advocates_loaded)}")

def main():
    AdvocatePipeline().run()
