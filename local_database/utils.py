import datetime
import json
import uuid

import redis

from config.config_provider import config


class BaseDB:
    def __init__(self):
        self.r = redis.StrictRedis(
            host="redis",
            charset="utf-8",
            decode_responses=True
        )

    def save(self, file_id: str, obj):
        return self.r.set(file_id, json.dumps(obj))

    def get(self, file_id: str):
        return json.loads(self.r.get(file_id))

    def delete(self, file_id: str):
        return self.r.delete(file_id)


class FileDBManager(BaseDB):

    def add_new_record(self, file_name, field_delimiter):
        file_id = str(uuid.uuid4())
        file = {
            "file_name": file_name,
            "field_delimiter": field_delimiter,
            "lock": False,
            "last_fragment_block_size": 1024,
            "key_ranges": [],
            "file_fragments": [],
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
        }
        self.save(file_id, file)

        return file_id

    def update(self, file_id, attrs):
        file_in_db = self.get(file_id)

        for key, value in attrs.items():
            if hasattr(file_in_db, key):
                setattr(file_in_db, key, value)

        self.save(file_id, file_in_db)
        return file_in_db

    def get_list_of_data_nodes_ip_addresses(self, file_id: str):
        file_in_db = self.get(file_id)
        data_nodes_ids = {list(data_node_id.keys())[0] for data_node_id in file_in_db["file_fragments"]}
        data_nodes_ip_addresses = [config.get_data_node_ip(data_node_id) for data_node_id in data_nodes_ids]

        return data_nodes_ip_addresses


class ShuffleDBManager(BaseDB):
    def __init__(self):
        super().__init__()
        self.r = redis.StrictRedis(host="redis",
                                   db=1,
                                   charset="utf-8",
                                   decode_responses=True)

    def add_new_record(self, file_id: str):
        file = {
            "list_of_min_hashes": [],
            "list_of_max_hashes": [],
            "data_nodes_processed": 0,
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
        }
        self.save(file_id, file)

        return file_id

    def reset_obj(self, file_id: str):
        file_db_obj = self.get(file_id)
        if file_db_obj:
            file_db_obj.update(
                {
                    "list_of_min_hashes": [],
                    "list_of_max_hashes": [],
                    "data_nodes_processed": 0,
                }
            )

            self.save(file_id, file_db_obj)
