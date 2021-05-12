import datetime
from typing import Any, Optional

from pydantic import BaseModel


class FileSchema(BaseModel):
    field_delimiter: Optional[str]
    file_name: Optional[str]
    file_id: Optional[str]


class RefreshTableRequest(BaseModel):
    file_id: str
    ip: str
    segment_name: str


class PhaseRequest(BaseModel):
    file_id: str
    field_delimiter: str
    source_file: Any


class StartMapPhaseRequest(PhaseRequest):
    mapper: str


class StartShufflePhaseRequest(PhaseRequest):
    pass


class StartReducePhaseRequest(PhaseRequest):
    reducer: str


class HashRequest(BaseModel):
    file_id: str
    max_hash_value: int
    min_hash_value: int


class FileDBInfo(BaseModel):
    id: Any
    file_name: str
    key_ranges: list
    file_fragments: list
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True


class ClearDataRequest(BaseModel):
    file_id: str
    remove_all_data: bool
    folder_name: Optional[str]
