"""
 Copyright Duel 2025
"""
import base64
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.server.api import app

client = TestClient(app)


def make_auth(username="admin", password="advocate-data-analyser"):
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

@patch("src.server.api.datastore")
def test_health_ok(mock_datastore):
    mock_datastore.__bool__.return_value = True

    response = client.get("/health", headers=make_auth())

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("src.server.api.datastore")
def test_health_auth_required(mock_datastore):
    response = client.get("/health")  # no auth
    assert response.status_code == 401


@patch("src.server.api.datastore.get_advocate")
def test_get_user_success(mock_get_advocate):
    fake_user = {"user_id": "123", "name": "Alice"}
    mock_get_advocate.return_value = fake_user

    response = client.get("/users/123", headers=make_auth())

    assert response.status_code == 200
    assert response.json() == fake_user


@patch("src.server.api.datastore.get_advocate")
def test_get_user_not_found(mock_get_advocate):
    mock_get_advocate.return_value = None

    response = client.get("/users/unknown", headers=make_auth())

    assert response.status_code == 400
    assert response.json()["detail"] == "User not found"


@patch("src.server.api.datastore.calculate_top_advocates")
def test_top_advocates_conversions(mock_calc):
    mock_calc.return_value = [
        {"user_id": "1", "total_conversions": 100}
    ]

    response = client.get("/metrics/top-advocates?metric=conversions", headers=make_auth())

    assert response.status_code == 200
    assert response.json()["metric"] == "conversions"
    assert len(response.json()["results"]) == 1


@patch("src.server.api.datastore.calculate_top_advocates")
def test_top_advocates_engagement(mock_calc):
    mock_calc.return_value = [
        {"user_id": "1", "total_engagement": 500}
    ]

    response = client.get("/metrics/top-advocates?metric=engagement", headers=make_auth())

    assert response.status_code == 200
    assert response.json()["metric"] == "engagement"


def test_top_advocates_invalid_metric():
    response = client.get("/metrics/top-advocates?metric=unknown", headers=make_auth())
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid metric"


def test_swagger_requires_auth():
    response = client.get("/swagger")
    assert response.status_code == 401


def test_swagger_success():
    response = client.get("/swagger", headers=make_auth())
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
