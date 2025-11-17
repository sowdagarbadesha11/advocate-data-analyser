import pytest
from unittest.mock import patch
import mongomock

from src.datastore.mongo import MongoDatastore


@pytest.fixture
def mocked_mongo():
    """Patches MongoClient inside MongoDatastore to use mongomock."""
    with patch("src.datastore.mongo.MongoClient", new=mongomock.MongoClient):
        ds = MongoDatastore()
        ds.connect()
        yield ds
        ds.disconnect()


def sample_advocates():
    """Returns two simple advocate docs for testing."""
    return [
        {
            "user_id": "u1",
            "name": "Alice",
            "advocacy_programs": [
                {
                    "brand": "BrandA",
                    "total_sales_attributed": 100,
                    "tasks_completed": [
                        {"likes": 10, "comments": 2, "shares": 1, "reach": 1000}
                    ]
                }
            ]
        },
        {
            "user_id": "u2",
            "name": "Bob",
            "advocacy_programs": [
                {
                    "brand": "BrandB",
                    "total_sales_attributed": 400,
                    "tasks_completed": [
                        {"likes": 50, "comments": 10, "shares": 5, "reach": 3000}
                    ]
                }
            ]
        },
    ]

def test_add_and_get_advocate(mocked_mongo):
    ds = mocked_mongo

    ds.add_advocates(sample_advocates())

    user = ds.get_advocate("u1")
    assert user["user_id"] == "u1"
    assert user["name"] == "Alice"

    user2 = ds.get_advocate("u2")
    assert user2["name"] == "Bob"

    assert ds.get_advocate("missing") is None

def test_calculate_top_advocates_conversions(mocked_mongo):
    ds = mocked_mongo
    ds.add_advocates(sample_advocates())

    results = ds.calculate_top_advocates("conversions", limit=2)

    assert len(results) == 2
    assert results[0]["user_id"] == "u2"  # Bob has 400 sales
    assert results[1]["user_id"] == "u1"  # Alice has 100 sales

def test_calculate_top_advocates_engagement(mocked_mongo):
    ds = mocked_mongo
    ds.add_advocates(sample_advocates())

    results = ds.calculate_top_advocates("engagement", limit=2)

    # Bob: 50 + 10 + 5 = 65
    # Alice: 10 + 2 + 1 = 13
    assert results[0]["user_id"] == "u2"
    assert results[1]["user_id"] == "u1"

def test_calculate_brand_performance(mocked_mongo):
    ds = mocked_mongo
    ds.add_advocates(sample_advocates())

    results = ds.calculate_brand_performance()

    assert len(results) == 2

    brands = {r["brand"]: r for r in results}

    assert brands["BrandA"]["total_tasks"] == 1
    assert brands["BrandA"]["total_sales"] == 100
    assert brands["BrandA"]["engagement"]["likes"] == 10

    assert brands["BrandB"]["total_sales"] == 400
    assert brands["BrandB"]["total_tasks"] == 1

def test_calculate_outliers_sales(mocked_mongo):
    ds = mocked_mongo
    ds.add_advocates(sample_advocates())

    # Bob has 400, Alice has 100 → Bob is an outlier
    results = ds.calculate_outliers("sales", stddev=0.5)

    assert len(results) == 1
    assert results[0]["user_id"] == "u2"
    assert results[0]["value"] == 400

def test_calculate_outliers_engagement(mocked_mongo):
    ds = mocked_mongo
    ds.add_advocates(sample_advocates())

    # Bob: 65, Alice: 13 → Bob is outlier
    results = ds.calculate_outliers("engagement", stddev=0.5)

    assert len(results) == 1
    assert results[0]["user_id"] == "u2"
