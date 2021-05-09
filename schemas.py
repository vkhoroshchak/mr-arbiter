import datetime
from typing import Any, Optional

from pydantic import BaseModel


class FileSchema(BaseModel):
    file_name: Optional[str]
    file_id: Optional[str]


class RefreshTableRequest(BaseModel):
    file_id: str
    ip: str
    segment_name: str


class PhaseRequest(BaseModel):
    field_delimiter: str
    file_id: str


class StartMapPhaseRequest(PhaseRequest):
    mapper: str


class StartShufflePhaseRequest(PhaseRequest):
    pass


class StartReducePhaseRequest(PhaseRequest):
    reducer: str


class FileDBInfo(BaseModel):
    id: Any
    file_name: str
    key_ranges: list
    file_fragments: list
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True
