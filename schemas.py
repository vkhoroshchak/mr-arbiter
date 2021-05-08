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


class StartMapPhaseRequest(BaseModel):
    field_delimiter: str
    mapper: str
    server_source_file: Optional[str]
    source_file: Optional[str]


class StartShufflePhaseRequest(BaseModel):
    field_delimiter: str
    source_file: str


class StartReducePhaseRequest(BaseModel):
    field_delimiter: str
    reducer: str
    server_source_file: Optional[str]
    source_file: Optional[str]


class FileDBInfo(BaseModel):
    id: Any
    file_name: str
    key_ranges: list
    file_fragments: list
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True
