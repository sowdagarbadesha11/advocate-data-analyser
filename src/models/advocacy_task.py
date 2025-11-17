"""
 Copyright Duel 2025
"""
from pydantic import BaseModel, ConfigDict, HttpUrl

class AdvocacyTask(BaseModel):
    """
    A single completed task within an advocacy program.
    """
    model_config = ConfigDict(extra="forbid")

    task_id: str
    platform: str
    # Raise ValidationError on invalid urls
    post_url: HttpUrl

    # Default metrics properties if not provided to aid stats calculations
    likes: int = 0
    comments: int = 0
    shares: int = 0
    reach: int = 0