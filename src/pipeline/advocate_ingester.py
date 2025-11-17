"""
 Copyright Dual 2025
"""

import json5
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional, Generator

from pydantic import ValidationError

from src.datastore.datastore_manager import DatastoreManager
from src.datastore.mongo import MongoDatastore
from src.models.advocate import Advocate
from src.pipeline.ingest_stats import IngestStats
from src.utilities.cleaning_utils import CleaningUtils

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

class AdvocateIngester:
    """
     A class to clean and pipeline advocate raw_data
    """

    def __init__(self, ingest_dir: Path, write_to_datastore: bool, max_workers: int = 8):
        """
        Constructor
        :param ingest_dir: The directory to load and ingest advocate raw_data from
        :param write_to_datastore: Whether to write to the datastore
        :param max_workers: The maximum number of workers to use for parallel processing
        """
        self.ingest_dir = ingest_dir
        self.advocates: List[Advocate] = []
        self.stats = IngestStats()
        self.write_to_datastore = write_to_datastore
        if self.write_to_datastore:
            self.datastore = DatastoreManager(MongoDatastore())
        self.max_workers = max_workers
        # Directory sibling to the original ingest folder for malformed and cleaned records
        self.invalid_json_dir = self.ingest_dir.parent / f"{self.ingest_dir.name}_invalid_json"
        self.failed_validation_dir = self.ingest_dir.parent / f"{self.ingest_dir.name}_failed_validation"

    def _iter_candidate_files(self) -> Generator[Path, None, Iterator[Any] | None]:
        """
        Yield JSON files that look like advocate payloads.

        Skips known junk/metadata files such as macOS '._*' resource-fork files.
        :return: A generator yielding Path objects for candidate files.
        """
        if not self.ingest_dir.is_dir():
            logger.warning("Ingest directory does not exist or is not a directory: %s", self.ingest_dir)
            return iter(())

        for path in self.ingest_dir.glob("*.json"):
            self.stats.files_seen += 1
            # Skip macOS resource-fork files like '._user_123.json'
            if path.name.startswith("._"):
                self.stats.files_skipped += 1
                continue
            yield path
        return None

    def _load_json(self, path: Path) -> Optional[Any]:
        """
        Load a JSON file, returning None if it's not valid JSON.

        This is defensive: many of the provided example files are binary
        macOS metadata masquerading as .json.
        :param path: The path to the file to load
        :return: The loaded JSON payload, or None if it's not valid JSON.
        """
        try:
            text = path.read_text(encoding="utf-8", errors="strict")
        except UnicodeDecodeError:
            logger.debug(f"Skipping non-text file: {path}")
            return None
        except OSError as exc:
            logger.warning(f"Failed reading {path}: {exc}")
            return None

        try:
            with open(path) as f:
                return json.load(f)
        except json.JSONDecodeError:
            # Fall back to slower, permissive loader
            with open(path) as f:
                try:
                    # Json5 handles trailing commas, missing quotes, etc.
                    return json5.load(f)
                except Exception as exc:
                    logger.debug(f"Error attempting to parse json for file: {path}. Error: {exc}")
                    self._write_invalid_json_record(path, text)
                return None

    @staticmethod
    def _clean_advocate(raw: dict) -> dict:
        """
        Cleans the raw advocate data by extracting and processing relevant fields.
        This method processes fields such as email, social media handles, and dates
        using utility functions. It also cleans and processes data from associated
        advocacy programs.
        :param raw: A dictionary containing raw advocate data.
        :return: A dictionary containing cleaned advocate data.
        """

        cleaned = {"user_id": raw.get("user_id"), "name": raw.get("name"),
                   "email": CleaningUtils.clean_email(raw.get("email")),
                   "instagram_handle": CleaningUtils.clean_handle(raw.get("instagram_handle")),
                   "tiktok_handle": CleaningUtils.clean_handle(raw.get("tiktok_handle")),
                   "joined_at": CleaningUtils.clean_date(raw.get("joined_at"))}

        programs = []
        for p in raw.get("advocacy_programs", []):
            programs.append(AdvocateIngester._clean_program(p))

        cleaned["advocacy_programs"] = programs

        return cleaned

    @staticmethod
    def _clean_program(program: dict) -> dict:
        """
        Cleans a program dictionary.
        :param program: Program data dictionary that needs to be cleaned.
        :return: A sanitized program dictionary with processed keys and values.
        """
        return {
            "program_id": program.get("program_id") or None,
            "brand": str(program.get("brand")) if program.get("brand") is not None else None,
            "total_sales_attributed": CleaningUtils.clean_float(program.get("total_sales_attributed")),
            "tasks_completed": [AdvocateIngester._clean_task(task) for task in program.get("tasks_completed", [])]
        }

    @staticmethod
    def _clean_task(task: dict) -> dict:
        """
        Cleans and standardizes a task dictionary by extracting and processing
        specific required keys.

        :param task: A dictionary containing information about a task.
        :return: A dictionary containing cleaned and standardized task
            information.
        """
        return {
            "task_id": task.get("task_id"),
            "platform": task.get("platform"),
            "post_url": CleaningUtils.clean_url(task.get("post_url")),
            "likes": CleaningUtils.clean_int(task.get("likes")),
            "comments": CleaningUtils.clean_int(task.get("comments")),
            "shares": CleaningUtils.clean_int(task.get("shares")),
            "reach": CleaningUtils.clean_int(task.get("reach")),
        }


    @staticmethod
    def _validate_advocate(clean_advocate_data: dict) -> Optional[Advocate]:
        """
        Validate cleaned raw_data into an Advocate model.
        :param clean_advocate_data: The clean raw_data to validate
        """
        return Advocate.model_validate(clean_advocate_data)

    def _write_invalid_json_record(self, source_path: Path, invalid_json: str) -> None:
        """
        Writes an invalid JSON record to a specified directory for logging and debugging purposes.
        :param source_path: Path to the source file that contains the invalid JSON.
        :param invalid_json: The invalid JSON content to be written to the file.
        """
        try:
            self.invalid_json_dir.mkdir(parents=True, exist_ok=True)
        except OSError as mkdir_exc:
            logger.warning(f"Failed to create invalid json directory {self.invalid_json_dir}: {mkdir_exc}")
            return

        out_name = f"{source_path.stem}_record_invalid_json.txt"
        out_path = self.invalid_json_dir / out_name

        try:
            out_path.write_text(json.dumps(invalid_json, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as write_exc:
            logger.warning(f"Failed to write invalid json record to {out_path}: {write_exc}")

    def _write_failed_validation_record(self, source_path: Path, cleaned: dict, exc: ValidationError) -> None:
        """
        Persist a malformed record to JSON for later analysis.
        :param source_path: The path to the original file
        :param cleaned: The cleaned record that failed validation
        :param exc: The ValidationError that was raised during validation
        """
        try:
            self.failed_validation_dir.mkdir(parents=True, exist_ok=True)
        except OSError as mkdir_exc:
            logger.warning(f"Failed to create failed validation directory {self.failed_validation_dir}: {mkdir_exc}")
            return

        out_name = f"{source_path.stem}_record_invalid.json"
        out_path = self.failed_validation_dir / out_name

        payload = {
            "source_file": str(source_path),
            "validation_errors": exc.errors(),
            "record": cleaned,
        }

        try:
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as write_exc:
            logger.warning(f"Failed to write invalid record to {out_path}: {write_exc}")

    def _process_file(self, path: Path) -> List[Advocate]:
        """
        Process a single file: load, clean and validate records.
        :return: A list of valid Advocate instances.
        """
        payload = self._load_json(path)
        if payload is None:
            self.stats.files_skipped += 1
            return []

        self.stats.files_parsed += 1
        advocates: List[Advocate] = []

        # Many platforms emit either a single object or a list of objects
        records: Iterable[Any]
        if isinstance(payload, list):
            records = payload
        else:
            records = [payload]

        for record in records:
            cleaned = AdvocateIngester._clean_advocate(record)
            if not cleaned:
                self.stats.records_invalid += 1
                logger.warning(
                    "Skipping non-dict or uncleanable record in %s at index %d",
                    path
                )
                continue

            try:
                model = self._validate_advocate(cleaned)
            except ValidationError as exc:
                self.stats.records_invalid += 1
                # # High-level summary
                # logger.warning(
                #     "Validation failed for record in %s: %s",
                #     path,
                #     exc,
                # )
                # # Structured details for debugging/cleaning rules
                # logger.debug(
                #     "Validation errors for %s: errors=%s, payload=%r",
                #     path,
                #     exc.errors(),
                #     cleaned,
                # )
                # Write malformed record to sidecar directory
                searlised_dates_data = CleaningUtils.serialise_dates(cleaned)
                self._write_failed_validation_record(path, searlised_dates_data, exc)
                continue

            advocates.append(model)
            self.stats.records_valid += 1

        return advocates

    def run(self) -> IngestStats:
        """
        Executes the ingestion process for Advocate JSON files from the specified directory.
        This includes identifying candidate JSON files, validating their content, and
        storing validated Advocate models into the datastore, if available.

        :return: `IngestStats` containing processing statistics, such as
                 number of files seen, parsed, skipped, and counts of valid or invalid records.
        :rtype: IngestStats
        """
        logger.info(f"Starting ingest from {self.ingest_dir}")
        self.advocates.clear()
        # Reset stats
        self.stats = IngestStats()

        advocate_files = list(self._iter_candidate_files())
        if not advocate_files:
            logger.info(f"No candidate JSON files found in {self.ingest_dir}")
            return self.stats

        batch = []  # stores validated Advocate models

        futures = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for path in advocate_files:
                futures.append(executor.submit(self._process_file, path))

            for future in as_completed(futures):
                advocates = future.result()
                for advocate in advocates:
                    if self.datastore:
                        batch.append(advocate.model_dump(mode="json"))
                        # Flush when batch gets big
                        if len(batch) >= BATCH_SIZE:
                            self.datastore.add_advocates(batch)
                            batch.clear()
                    else:
                        # Dry-run mode
                        self.advocates.append(advocate)
        # Final flush
        if batch and self.datastore:
            self.datastore.add_advocates(batch)
            batch.clear()

        logger.info(
            "Ingest complete: files_seen=%d, files_parsed=%d, files_skipped=%d, "
            "records_valid=%d, records_invalid=%d",
            self.stats.files_seen,
            self.stats.files_parsed,
            self.stats.files_skipped,
            self.stats.records_valid,
            self.stats.records_invalid,
        )

        return self.stats