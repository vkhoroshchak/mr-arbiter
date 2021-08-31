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


class StartMapPhaseRequest(PhaseRequest):
    mapper: str


class StartShufflePhaseRequest(PhaseRequest):
    pass


class StartReducePhaseRequest(PhaseRequest):
    reducer: str
    source_file: Any


class HashRequest(BaseModel):
    file_id: str
    min_hash_value: int
    max_hash_value: int
    # list_keys: list
    # field_delimiter: str


class FileDBInfo(BaseModel):
    file_name: str
    key_ranges: list
    file_fragments: list
    created_at: str
    updated_at: str

    class Config:
        orm_mode = True


class ClearDataRequest(BaseModel):
    file_id: str
    remove_all_data: bool
    folder_name: Optional[str]
