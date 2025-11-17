"""
 Copyright Dual 2025
"""
from pydantic import BaseModel, ConfigDict, Field
from typing import List

from src.models.advocacy_task import AdvocacyTask


class AdvocacyProgram(BaseModel):
    """
    A program an advocate participates in, with completed tasks.
    """
    model_config = ConfigDict(extra="forbid")

    program_id: str
    brand: str

    # Optional in case advocates are not yet assigned tasks
    tasks_completed: List[AdvocacyTask] = Field(default_factory=list)

    total_sales_attributed: float