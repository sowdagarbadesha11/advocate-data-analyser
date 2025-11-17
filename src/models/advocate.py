"""
 Copyright Duel 2025
"""
from __future__ import annotations

from datetime import datetime
import logging
from typing import Optional, List
import re
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from src.models.advocacy_program import AdvocacyProgram

logger = logging.getLogger(__name__)


class Advocate(BaseModel):
    """
    A model representing an advocate.

    This is intentionally minimal for now; extend as the schema solidifies.
    """
    # Allow unknown fields for now whilst exploring the data
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    # Core identity fields – required
    name: str
    user_id: str

    # Contact / profile fields – optional
    email: Optional[EmailStr] = None
    joined_at: Optional[datetime] = None

    # Social media handles
    instagram_handle: Optional[str] = None
    tiktok_handle: Optional[str] = None

    # Nested structure for programs and tasks
    advocacy_programs: List[AdvocacyProgram] = Field(default_factory=list)

    @staticmethod
    def _clean_handle(value: Optional[str]) -> Optional[str]:
        """
        Normalises social handles into a clean format:
        - Returns None for empty input
        - Strips whitespace
        - Forces lowercase
        - Removes invalid characters
        - Ensures leading '@'
        """
        if not value:
            return None

        original = value
        value = value.strip().lower()

        # Remove undesirable characters
        # Allow letters, numbers, underscore and '@'
        value = re.sub(r"[^a-z0-9_@]", "", value)

        if not value:
            # The entire string was junk
            return None

        # Ensure it starts with '@'
        if not value.startswith("@"):
            value = "@" + value

        # Log if it was changed significantly
        if value != original:
            logger.debug(f"Normalised social handle '{original}' → '{value}'")

        return value

    @field_validator("instagram_handle", "tiktok_handle", mode="before")
    @classmethod
    def validate_social_handle(cls, value):
        return cls._clean_handle(value)
