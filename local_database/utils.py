from fastapi import status, HTTPException
from sqlalchemy.exc import StatementError

from local_database.models import Session, FileDB


class FileDBManager:
    def __init__(self):
        self.session = Session()

    def save(self, obj):
        self.session.add(obj)
        self.session.commit()
        self.session.refresh(obj)

        return obj

    def add_new_record(self, file_name):
        file_in_db = FileDB(file_name=file_name)
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
