"""
 Copyright Duel 2025
"""
import re
import datetime

from urllib.parse import urlparse


class CleaningUtils:
    """
     A utilities class for cleaning advocate raw_data
    """

    @staticmethod
    def clean_email(value):
        if not value or "@" not in value:
            return None
        value = value.strip().lower()
        if re.match(r"^[^@]+@[^@]+\.[^@]+$", value):
            return value
        return None

    @staticmethod
    def clean_handle(value):
        if not value:
            return None

        # Remove weird characters
        value = value.strip().lower()
        value = re.sub(r"[^a-z0-9_@]", "", value)

        # Normalise: ensure single '@' at start
        if not value.startswith("@"):
            value = "@" + value

        return value

    @staticmethod
    def clean_date(value):
        if not value:
            return None
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def clean_int(value):
        if value is None:
            return 0
        try:
            return int(value)
        except Exception:
            return 0

    @staticmethod
    def clean_float(value):
        if value is None:
            return 0.0
        try:
            return float(value)
        except Exception:
            return 0.0

    @staticmethod
    def clean_url(value):
        if not value or not isinstance(value, str):
            return None
        value = value.strip()
        parsed = urlparse(value)
        if parsed.scheme and parsed.netloc:
            return value
        return None

    @staticmethod
    def serialise_dates(obj):
        if isinstance(obj, dict):
            return {k: CleaningUtils.serialise_dates(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [CleaningUtils.serialise_dates(v) for v in obj]
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return obj