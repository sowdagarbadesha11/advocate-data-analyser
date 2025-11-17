"""
 Copyright Dual 2025
"""
from abc import ABC, abstractmethod


class Datastore(ABC):
    """
    An abstract datastore class to be implemented with different database technologies
    """

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def add_advocates(self, advocates: dict):
        pass

    @abstractmethod
    def get_advocate(self, user_id: str):
        pass

    @abstractmethod
    def calculate_top_advocates(self, metric, limit) -> list:
        pass

    @abstractmethod
    def calculate_brand_performance(self) -> list:
        pass

    @abstractmethod
    def calculate_outliers(self, metric, stddev):
        pass