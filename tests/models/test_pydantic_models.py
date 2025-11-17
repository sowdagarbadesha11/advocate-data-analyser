"""
 Copyright Duel 2025
"""
import pytest
from pydantic import ValidationError
from datetime import datetime

from src.models.advocate import Advocate
from src.models.advocacy_program import AdvocacyProgram
from src.models.advocacy_task import AdvocacyTask

@pytest.fixture
def valid_task():
    return {
        "task_id": "1234",
        "platform": "Instagram",
        "post_url": "https://example.com/post",
        "likes": 100,
        "comments": 5,
        "shares": 10,
        "reach": 5000,
    }


@pytest.fixture
def valid_program(valid_task):
    return {
        "program_id": "abcd",
        "brand": "TestBrand",
        "total_sales_attributed": 123.45,
        "tasks_completed": [valid_task],
    }


@pytest.fixture
def valid_user(valid_program):
    return {
        "user_id": "user-1",
        "name": "Alice Example",
        "email": "alice@example.com",
        "instagram_handle": "@alice",
        "tiktok_handle": "@alice_tk",
        "joined_at": "2024-02-01T00:00:00Z",
        "advocacy_programs": [valid_program],
    }


# -----------------------------------------------------
# TASK MODEL TESTS
# -----------------------------------------------------

def test_task_valid(valid_task):
    task = AdvocacyTask(**valid_task)
    assert task.likes == 100
    assert task.comments == 5
    assert task.shares == 10
    assert task.reach == 5000


def test_task_invalid_url(valid_task):
    invalid = valid_task.copy()
    invalid["post_url"] = "not-a-url"
    with pytest.raises(ValidationError):
        AdvocacyTask(**invalid)


def test_task_missing_required_field(valid_task):
    invalid = valid_task.copy()
    invalid.pop("task_id")
    with pytest.raises(ValidationError):
        AdvocacyTask(**invalid)


def test_task_number_coercion(valid_task):
    valid_task["likes"] = "250"
    task = AdvocacyTask(**valid_task)
    assert task.likes == 250


# -----------------------------------------------------
# PROGRAM MODEL TESTS
# -----------------------------------------------------

def test_program_valid(valid_program):
    program = AdvocacyProgram(**valid_program)
    assert program.brand == "TestBrand"
    assert len(program.tasks_completed) == 1


def test_program_invalid_sales_number(valid_program):
    invalid = valid_program.copy()
    invalid["total_sales_attributed"] = "bad-num"
    with pytest.raises(ValidationError):
        AdvocacyProgram(**invalid)


def test_program_tasks_nested_validation(valid_program):
    invalid = valid_program.copy()
    invalid["tasks_completed"] = [{
        "task_id": None,
        "platform": "TikTok",
        "post_url": "https://x.com",
        "likes": 10,
        "comments": 2,
        "shares": 1,
        "reach": 100
    }]
    with pytest.raises(ValidationError):
        AdvocacyProgram(**invalid)


# -----------------------------------------------------
# ADVOCATE MODEL TESTS
# -----------------------------------------------------

def test_advocate_valid(valid_user):
    user = Advocate(**valid_user)
    assert user.user_id == "user-1"
    assert user.email == "alice@example.com"
    assert len(user.advocacy_programs) == 1


def test_advocate_invalid_email(valid_user):
    invalid = valid_user.copy()
    invalid["email"] = "bad-email"
    with pytest.raises(ValidationError):
        Advocate(**invalid)


def test_advocate_missing_required(valid_user):
    invalid = valid_user.copy()
    invalid.pop("name")
    with pytest.raises(ValidationError):
        Advocate(**invalid)


def test_advocate_nested_program_validation(valid_user):
    invalid = valid_user.copy()
    invalid["advocacy_programs"] = [{
        "program_id": "xyz",
        "brand": "Brand",
        "total_sales_attributed": 999,
        "tasks_completed": [{
            # task_id missing → invalid
            "platform": "Instagram",
            "post_url": "https://example.com",
            "likes": 1,
            "comments": 1,
            "shares": 1,
            "reach": 1
        }]
    }]

    with pytest.raises(ValidationError):
        Advocate(**invalid)


def test_advocate_date_parsing(valid_user):
    user = Advocate(**valid_user)
    assert isinstance(user.joined_at, datetime)

def test_handle_normalisation(valid_user):
    valid_user["instagram_handle"] = "  Alice__99 "
    user = Advocate(**valid_user)
    assert user.instagram_handle == "@alice__99"

def test_handle_removes_invalid_chars(valid_user):
    valid_user["instagram_handle"] = "###Bad!!"
    user = Advocate(**valid_user)
    assert user.instagram_handle == "@bad"

def test_handle_none(valid_user):
    valid_user["instagram_handle"] = None
    user = Advocate(**valid_user)
    assert user.instagram_handle is None

def test_handle_invalid_becomes_none(valid_user):
    valid_user["instagram_handle"] = "!!!!"
    user = Advocate(**valid_user)
    assert user.instagram_handle is None
