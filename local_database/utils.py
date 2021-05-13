from contextlib import contextmanager

from fastapi import status, HTTPException
from sqlalchemy.exc import StatementError

from config.config_provider import config
from local_database.models import Session, FileDB, Shuffle


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


class BaseDB:
    def __init__(self, session=None):
        self.session = session

    def save(self, obj):
        self.session.add(obj)
        self.session.commit()
        self.session.refresh(obj)
        return obj


class FileDBManager(BaseDB):

    def add_new_record(self, file_name, field_delimiter):
        file_in_db = FileDB(file_name=file_name, field_delimiter=field_delimiter)
        self.save(file_in_db)

        return file_in_db

    def get_file_by_id(self, file_id):
        try:
            file_obj: FileDB = (
                self.session.query(FileDB)
                            .filter(FileDB.id == file_id)
                            .first()
            )
        except StatementError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid file id"
            )
        else:
            return file_obj

    def update(self, file_id, attrs):
        file_in_db = self.get_file_by_id(file_id)

        for key, value in attrs.items():
            if hasattr(file_in_db, key):
                setattr(file_in_db, key, value)

        self.save(file_in_db)
        return file_in_db

    def get_list_of_data_nodes_ip_addresses(self, file_id: str):
        file_in_db = self.get_file_by_id(file_id)
        data_nodes_ids = {list(data_node_id.keys())[0] for data_node_id in file_in_db.file_fragments}
        data_nodes_ip_addresses = [config.get_data_node_ip(data_node_id) for data_node_id in data_nodes_ids]

        return data_nodes_ip_addresses


class ShuffleDBManager(BaseDB):

    def add_new_record(self, file_id: str):
        file_in_db = Shuffle(file_id=file_id)
        self.save(file_in_db)

        return file_in_db

    def get_shuffle_obj_by(self, file_id: str):
        try:
            shuffle_obj: Shuffle = (
                self.session.query(Shuffle)
                    .filter(Shuffle.file_id == file_id)
                    .first()
            )
        except StatementError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid file id"
            )
        else:
            return shuffle_obj

    def delete(self, file_id: str):
        file_db_obj = self.get_shuffle_obj_by(file_id)
        if file_db_obj:
            self.session.delete(file_db_obj)
            self.session.commit()

            return True
        else:
            return False

    def reset_obj(self, file_id: str):
        file_db_obj = self.get_shuffle_obj_by(file_id)
        if file_db_obj:
            file_db_obj.data_nodes_processed = 0
            file_db_obj.list_of_max_hashes = []
            file_db_obj.list_of_min_hashes = []
            self.save(file_db_obj)
