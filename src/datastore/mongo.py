"""
 Copyright Dual 2025
"""
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.datastore.datastore import Datastore
import logging
import os
import numpy as np

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
load_dotenv()

# Parse environment variables
DATABASE_URI = os.environ.get("MONGO_URI")
DATABASE_HOST = os.environ.get("MONGO_HOST")
DATABASE_PORT = os.environ.get("MONGO_PORT")
DATABASE_USERNAME = os.environ.get("MONGO_USERNAME")
DATABASE_PASSWORD = os.environ.get("MONGO_PASSWORD")
DATABASE_NAME = os.environ.get("MONGO_DATABASE")

logger = logging.getLogger(__name__)

class MongoDatastore(Datastore):
    """
    Class for managing MongoDB operations
    """

    def __init__(self):
        """
        Constructor
        """
        self.client = None
        self.database = None
        self.collection = None

    def connect(self) -> None:
        """
        Connects to the MongoDB database and initializes the client, database, and collection.
        """
        try:
            self.client = MongoClient(f"{DATABASE_URI}{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}")
            self.database = self.client[DATABASE_NAME]
            self.collection = self.database["advocates"]
            # Index collection using user_id for speed
            self.collection.create_index("user_id")
        except ConnectionError as conn_err:
            logger.error("Error connecting to MongoDB: %s", conn_err)

    def disconnect(self) -> None:
        """
        Disconnects the client connection to the MongoDatastore if it exists.
        """
        if self.client:
            self.client.close()
        else:
            logger.info("MongoDatastore not connected")

    def add_advocates(self, advocates: dict) -> None:
        """
        Adds multiple advocate records to the database. This method inserts the provided
        dictionary of advocate records into the MongoDB collection and ensures a
        database index on the "user_id" field for optimized querying.

        :param advocates: Dictionary containing advocate records to be inserted
        """
        try:
            self.collection.insert_many(advocates)
        except PyMongoError as e:
            logger.error("Error connecting to MongoDB: %s", e)
        except Exception as e:
            logger.error("Error inserting advocates: %s", e)

    def get_advocate(self, user_id: str) -> dict:
        """
        Retrieves an advocate's information from the database using the given user ID.

        :param user_id: The unique identifier for the advocate to search for.
        :return: A dictionary containing advocate information from the database.
        """
        collection = self.database["advocates"]
        user = collection.find_one({"user_id": user_id}, {"_id": 0})
        return user

    def calculate_top_advocates(self, metric, limit) -> list:
        """
        Calculates and fetches the top advocates based on a specified performance metric
        (either "conversions" or "engagement") and limits the result to the specified number.

        :param metric: The performance metric to calculate the top advocates by.
        :param limit: The maximum number of top advocates to retrieve.
        :return: A list of dictionaries representing the top advocates, each containing
                 their user details as well as the calculated performance metric.
        """
        # Calculates a simple sum of program-level attribution
        if metric == "conversions":
            pipeline = [
                {"$unwind": "$advocacy_programs"},
                {
                    "$group": {
                        "_id": "$user_id",
                        "total_conversions": {
                            "$sum": "$advocacy_programs.total_sales_attributed"
                        }
                    }
                },
                {"$sort": {"total_conversions": -1}},
                {"$limit": limit},
                {"$project": {"_id": 0, "user_id": "$_id", "total_conversions": 1}}
            ]

            results = list(self.collection.aggregate(pipeline))

            # Rename consistently with API expectations
            return [
                {
                    "user_id": r["user_id"],
                    "value": r["total_conversions"]
                }
                for r in results
            ]

        # Calculates engagement as a sum of likes, comments, shares across ALL tasks
        if metric == "engagement":
            pipeline = [
                {"$unwind": "$advocacy_programs"},
                {"$unwind": "$advocacy_programs.tasks_completed"},
                {
                    "$group": {
                        "_id": "$user_id",
                        "total_engagement": {
                            "$sum": {
                                "$add": [
                                    "$advocacy_programs.tasks_completed.likes",
                                    "$advocacy_programs.tasks_completed.comments",
                                    "$advocacy_programs.tasks_completed.shares"
                                ]
                            }
                        }
                    }
                },
                {"$sort": {"total_engagement": -1}},
                {"$limit": limit},
                {"$project": {"_id": 0, "user_id": "$_id", "total_engagement": 1}}
            ]

            results = list(self.collection.aggregate(pipeline))

            return [
                {
                    "user_id": r["user_id"],
                    "value": r["total_engagement"]
                }
                for r in results
            ]

        # Unsupported metric
        return []

    def calculate_brand_performance(self) -> list:
        """
        Aggregates program + task metrics across all advocates grouped by brand.
        """
        pipeline = [
            {"$unwind": "$advocacy_programs"},
            {"$unwind": "$advocacy_programs.tasks_completed"},
            {
                "$group": {
                    "_id": "$advocacy_programs.brand",
                    "total_tasks": {"$sum": 1},
                    "total_likes": {"$sum": "$advocacy_programs.tasks_completed.likes"},
                    "total_comments": {"$sum": "$advocacy_programs.tasks_completed.comments"},
                    "total_shares": {"$sum": "$advocacy_programs.tasks_completed.shares"},
                    "total_reach": {"$sum": "$advocacy_programs.tasks_completed.reach"},
                    "total_sales": {"$sum": "$advocacy_programs.total_sales_attributed"},
                }
            },
            {"$sort": {"total_sales": -1}},
            {
                "$project": {
                    "_id": 0,
                    "brand": "$_id",
                    "total_tasks": 1,
                    "engagement": {
                        "likes": "$total_likes",
                        "comments": "$total_comments",
                        "shares": "$total_shares"
                    },
                    "total_reach": "$total_reach",
                    "total_sales": "$total_sales"
                }
            }
        ]

        return list(self.collection.aggregate(pipeline))


    def calculate_outliers(self, metric, stddev):
        """
        Calculates outliers based on a specified metric and standard deviation limit.

        :param metric: The metric to evaluate for outlier detection. Allowed values are 'sales' or 'engagement'.
        :param stddev: The number of standard deviations beyond which a value is considered an outlier.
        :return: A list of dictionaries containing user IDs and their respective metric values for those identified
        as outliers.
        """
        # Flatten metrics for all advocates
        advocates = list(self.collection.find({}, {
            "user_id": 1,
            "advocacy_programs": 1
        }))

        dataset = []

        for advocate in advocates:
            programs = advocate.get("advocacy_programs", [])

            # SALES metric
            if metric == "sales":
                total = sum(p.get("total_sales_attributed", 0) for p in programs)

            # ENGAGEMENT metric
            else:
                total = 0
                for p in programs:
                    for t in p.get("tasks_completed", []):
                        total += (
                                t.get("likes", 0) +
                                t.get("comments", 0) +
                                t.get("shares", 0)
                        )

            dataset.append({"user_id": advocate.get("user_id"), "value": total})

        values = np.array([d["value"] for d in dataset])

        if len(values) == 0:
            return []

        mean = values.mean()
        sigma = values.std()

        upper_limit = mean + stddev * sigma

        return [
            d for d in dataset
            if d["value"] > upper_limit
        ]
