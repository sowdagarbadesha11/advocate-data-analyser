"""
 Copyright Dual 2025
"""
import uvicorn


def main():
    """
    Starts and configures the Uvicorn server to run the FastAPI application.
    """
    uvicorn.run("src.server.api:app", host="0.0.0.0", port=8000, reload=True)