"""
 Copyright Duel 2025
"""
from dataclasses import dataclass


@dataclass
class IngestStats:
    """
    Simple stats for a single run.
    """
    files_seen: int = 0
    files_parsed: int = 0
    files_skipped: int = 0
    records_valid: int = 0
    records_invalid: int = 0