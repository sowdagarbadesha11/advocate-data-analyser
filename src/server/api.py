"""
 Copyright Duel 2025
"""
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

import logging

import bcrypt
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from src.datastore.datastore_manager import DatastoreManager
from src.datastore.mongo import MongoDatastore

app = FastAPI(
    title="Duel Take-Home API",
    description="API exposing cleaned advocate data and aggregated metrics.",
    version="1.0.0"
)

# Set up basic authentication
security = HTTPBasic()
users = {"admin": b"$2a$12$IqjOj2RTumAaEYO1ibqgqOMcLDdnQZOhaP4cEaNTyBuGfvEzfsYCy"}

logger = logging.getLogger(__name__)

# Allow Swagger UI and any local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

datastore = DatastoreManager(MongoDatastore())

def verify_password(credentials: HTTPBasicCredentials = Depends(security)):
    """
    Password verification.
    :param credentials: The entered credentials.
    :return: The username if the credentials are valid, else None.
    """
    correct_login = credentials.username in users and bcrypt.checkpw(credentials.password.encode("utf-8"),
                                                                     users.get(credentials.username))
    if not correct_login:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    return credentials.username

@app.get("/health", dependencies=[Depends(verify_password)])
def health():
    """
    Checks the health status of the service and verifies whether the datastore
    is available and functioning correctly.
    :return: A dictionary containing the health status of the service.
    """
    if datastore:
        return {"status": "ok"}
    return {"status": "error"}


@app.get("/users/{user_id}", dependencies=[Depends(verify_password)])
def get_user(user_id: str):
    """
     Get advocate user by ID
    :param user_id: the user ID
    :return: the user object
    """
    user = datastore.get_advocate(user_id)
    if not user:
        raise HTTPException(status_code=400, detail="User not found")
    return user


@app.get("/metrics/top-advocates", dependencies=[Depends(verify_password)])
def top_advocates(limit: Optional[int] = 20, metric: Optional[str] = "conversions"):
    """
    Retrieves the top advocates based on the specified metric.

    :param limit: The maximum number of advocates to be returned. Defaults to 20.
    :param metric: The metric to filter advocates by, either "conversions" or "engagement". Defaults to "conversions".
    :return: A dictionary containing the selected metric and a list of results from the datastore.
    :raises HTTPException: If the metric specified is invalid, a 400 status code is returned.
    """
    if metric not in ("conversions", "engagement"):
        raise HTTPException(status_code=400, detail="Invalid metric")

    results = datastore.calculate_top_advocates(metric, limit)

    return {"metric": metric, "results": results}

@app.get("/metrics/brands/performance", dependencies=[Depends(verify_password)])
async def brand_performance():
    """
    Calculates and returns the performance metrics of brands stored in the datastore. These metrics are:
    - total tasks completed
    - total engagement (likes + comments + shares)
    - total reach
    - total attributed sales
    :return: A JSON object containing the performance metrics of brands.
    """
    results = datastore.calculate_brand_performance()
    return {"results": results}

@app.get("/metrics/outliers")
async def outliers(metric: str, stddev: int = 2):
    """
    Retrieves statistical outliers for a specified metric and a standard deviation threshold.
    This endpoint calculates outliers based on the provided metric (e.g., 'sales' or
    'engagement') and the number of standard deviations to analyze.

    :param metric: A string indicating the metric name. Accepted values are 'sales' and 'engagement'.
    :param stddev: An integer representing the number of standard deviations to use in determining outliers. Defaults to 2.
    :return: A dictionary containing the provided metric, the standard deviation threshold, and the list of calculated outliers.
    :raises HTTPException: If an invalid metric is provided.
    """
    if metric not in ("sales", "engagement"):
        raise HTTPException(status_code=400, detail="Metric must be 'sales' or 'engagement'")

    results = datastore.calculate_outliers(metric, stddev)
    return {
        "metric": metric,
        "stddev": stddev,
        "outliers": results
    }

@app.get("/swagger", dependencies=[Depends(verify_password)])
def swagger():
    """
    Generates and returns a Swagger UI HTML for API documentation.
    :return: The Swagger UI HTML content for API documentation.
    """
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Duel Take-Home API")