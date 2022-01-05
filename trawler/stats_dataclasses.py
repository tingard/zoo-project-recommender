from typing import List
from datetime import datetime
from pydantic import BaseModel


# For time-based counts
class HourlyDocCount(BaseModel):
    doc_count: int
    key: int
    key_as_str: datetime

class ProjectEventsBuckets(BaseModel):
    bucket: List[HourlyDocCount]

class ProjectEventsOverTimeCount(BaseModel):
    events_over_time: ProjectEventsBuckets



