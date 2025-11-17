"""
 Copyright Dual 2025
"""
import logging

from src.datastore.datastore import Datastore
logger = logging.getLogger(__name__)

class DatastoreManager:
    """
    A datastore manager that acts as an interface for a concrete datastore object
    """

    def __init__(self, datastore: Datastore):
        """
        Constructor
        :param datastore: The datastore object to be used.
        """
        self.datastore = datastore
        self.datastore.connect()

    def __enter__(self):
        """
        Handles the context management for the object.
        :return: The instance of the context manager.
        """
        return self

    def __exit__(self, a, b, c):
        """
        Handles the cleanup and disconnection of the datastore when exiting a context
        using the context management protocol.
        :param a: exception type, if an exception was raised while inside the `with` block.
        :param b: exception value, if an exception was raised while inside the `with` block.
        :param c: traceback object, if an exception was raised while inside the `with` block.
        """
        self.datastore.disconnect()

    def add_advocates(self, advocates) -> None:
        """
        Adds advocates to the datastore.
        :param advocates: The data or list of advocates to be added to the datastore.
        """
        self.datastore.add_advocates(advocates)

    def get_advocate(self, user_id: str) -> dict:
        """
        Retrieve the advocate data for the provided user ID.
        :param user_id: The unique identifier for the user whose advocate data is to be retrieved.
        :return: A dictionary containing advocate data if found; otherwise returns None.
        """
        result = self.datastore.get_advocate(user_id)
        if not result:
            logging.warning(f"Advocate not found: {user_id}")
        return result

    def calculate_top_advocates(self, metric: str, limit: int) -> list:
        """
        Calculates a list of top advocates based on a specific metric and limit.
        :param metric: A string representing the metric used as the basis for ranking advocates.
        :param limit: The maximum number of advocates to be returned.
        :return: A list of advocates ranked by the specified metric and limited to the specified size.
        """
        result = self.datastore.calculate_top_advocates(metric, limit)
        if not result:
            logging.warning(f"Could not calculate top advocates for metric {metric} and limit {limit}")
        return result

    def calculate_brand_performance(self) -> list:
        """
        Calculates the performance of a brand by obtaining data from a datastore
        and logs a warning in case the calculation is unsuccessful.
        :return: A list containing the calculated brand performance data.
        """
        result = self.datastore.calculate_brand_performance()
        if not result:
            logging.warning(f"Could not calculate brand performance")
        return result

    def calculate_outliers(self, metric: str, stddev: float) -> list:
        """
        Calculate and return outliers based on the given metric and standard
        deviation threshold.
        :param metric: The target metric used to calculate the outliers.
        :param stddev: The standard deviation threshold for identifying outliers.
        :return: A list of outliers computed based on the provided metric and
            standard deviation.
        :rtype: list
        """
        result = self.datastore.calculate_outliers(metric, stddev)
        if not result:
            logging.warning(f"Could not calculate outliers for metric {metric} and stddev {stddev}")
        return result