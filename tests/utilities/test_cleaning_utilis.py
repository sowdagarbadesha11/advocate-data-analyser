"""
 Copyright Duel 2025
"""
import datetime

from src.utilities.cleaning_utils import CleaningUtils


class TestCleaningUtils:

    def test_clean_email_valid(self):
        assert CleaningUtils.clean_email("  USER@Test.COM ") == "user@test.com"

    def test_clean_email_invalid_format(self):
        assert CleaningUtils.clean_email("not-an-email") is None

    def test_clean_email_missing(self):
        assert CleaningUtils.clean_email(None) is None

    def test_clean_handle_valid(self):
        assert CleaningUtils.clean_handle("@User123") == "@user123"

    def test_clean_handle_missing(self):
        assert CleaningUtils.clean_handle("") is None

    def test_clean_handle_adds_at_symbol(self):
        assert CleaningUtils.clean_handle("user") == "@user"

    def test_clean_handle_strips_invalid_chars(self):
        assert CleaningUtils.clean_handle("#£$User!") == "@user"

    def test_clean_date_valid_iso(self):
        res = CleaningUtils.clean_date("2024-05-17T10:30:00Z")
        assert isinstance(res, datetime.datetime)
        assert res.year == 2024

    def test_clean_date_invalid(self):
        assert CleaningUtils.clean_date("not-a-date") is None

    def test_clean_date_none(self):
        assert CleaningUtils.clean_date(None) is None

    def test_clean_int_valid(self):
        assert CleaningUtils.clean_int("42") == 42
        assert CleaningUtils.clean_int(99) == 99

    def test_clean_int_invalid(self):
        assert CleaningUtils.clean_int("NaN") == 0

    def test_clean_int_missing(self):
        assert CleaningUtils.clean_int(None) == 0

    def test_clean_float_valid(self):
        assert CleaningUtils.clean_float("10.5") == 10.5
        assert CleaningUtils.clean_float(2.71) == 2.71

    def test_clean_float_invalid(self):
        assert CleaningUtils.clean_float("not-a-number") == 0.0

    def test_clean_float_missing(self):
        assert CleaningUtils.clean_float(None) == 0.0

    def test_clean_url_valid(self):
        assert CleaningUtils.clean_url("https://example.com") == "https://example.com"

    def test_clean_url_invalid(self):
        assert CleaningUtils.clean_url("broken_link") is None

    def test_clean_url_missing(self):
        assert CleaningUtils.clean_url(None) is None

    def test_serialise_dates_datetime(self):
        dt = datetime.datetime(2024, 5, 17, 12, 0)
        result = CleaningUtils.serialise_dates(dt)
        assert result == "2024-05-17T12:00:00"

    def test_serialise_dates_nested(self):
        obj = {
            "created": datetime.datetime(2024, 1, 1, 0, 0),
            "nested": {
                "joined": datetime.datetime(2025, 1, 1, 0, 0)
            },
            "items": [
                datetime.datetime(2023, 5, 20, 10, 0)
            ]
        }
        result = CleaningUtils.serialise_dates(obj)

        assert result["created"] == "2024-01-01T00:00:00"
        assert result["nested"]["joined"] == "2025-01-01T00:00:00"
        assert result["items"][0] == "2023-05-20T10:00:00"
