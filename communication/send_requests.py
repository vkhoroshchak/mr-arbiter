import requests

import schemas
from config.config_provider import config
from config.logger import arbiter_logger
from local_database.utils import FileDBManager, ShuffleDBManager, session_scope

logger = arbiter_logger.get_logger(__name__)


def create_config_and_filesystem(file_name):
    return send_request_to_data_nodes({'file_name': file_name}, 'create_config_and_filesystem')


def send_request_to_data_nodes(data_to_data_node, command):
    logger.info(f"Send request to data nodes: {data_to_data_node}")
    for item in config.data_nodes:
        url = f'http://{item["data_node_address"]}/command/{command}'
        requests.post(url, json=data_to_data_node)


def start_map_phase(map_request):
    return send_request_to_data_nodes(map_request.__dict__, 'map')


def start_reduce_phase(reduce_request):
    return send_request_to_data_nodes(reduce_request.__dict__, 'reduce')


def min_max_hash(shuffle_request: schemas.StartShufflePhaseRequest):
    return send_request_to_data_nodes(shuffle_request.__dict__, 'min_max_hash')


def generate_hash_ranges(hash_request: schemas.HashRequest):
    with session_scope() as session:
        logger.info("Received hash key ranges")
        logger.info(f"Min hash value: {hash_request.min_hash_value}")
        logger.info(f"Max hash value: {hash_request.max_hash_value}")

        shuffle_db_manager = ShuffleDBManager(session)
        shuffle_db_obj = shuffle_db_manager.get_shuffle_obj_by(hash_request.file_id)
        shuffle_db_obj.list_of_max_hashes.append(hash_request.max_hash_value)
        shuffle_db_obj.list_of_min_hashes.append(hash_request.min_hash_value)
        shuffle_db_obj.data_nodes_processed += 1
        shuffle_db_manager.save(shuffle_db_obj)

        logger.info(f"Data nodes processed: {shuffle_db_obj.data_nodes_processed}")

        file_db_manager = FileDBManager(session)
        file_db_obj = file_db_manager.get_file_by_id(hash_request.file_id)
        data_nodes_ip_addresses = file_db_manager.get_list_of_data_nodes_ip_addresses(hash_request.file_id)

        # If we get key hash ranges from all data nodes - start shuffle phase
        if shuffle_db_obj.data_nodes_processed == len(data_nodes_ip_addresses):
            logger.info("Received hash key ranges from all data nodes")
            max_hash = max(shuffle_db_obj.list_of_max)
            min_hash = min(shuffle_db_obj.list_of_min)
            step = (max_hash - min_hash) / len(data_nodes_ip_addresses)

            data_to_data_node = {
                'nodes_keys': [],
                'max_hash': max_hash,
                'file_name': file_db_obj.file_name,
                'field_delimiter': file_db_obj.field_delimiter,
            }
            mid_hash = min_hash

            for index, data_node in enumerate(config.data_nodes):
                if index == len(data_nodes_ip_addresses):
                    end_hash = max_hash
                else:
                    end_hash = mid_hash + step
                data_to_data_node['nodes_keys'].append({
                    'data_node_ip': data_node['data_node_address'],
                    'hash_keys_range': [mid_hash, end_hash]
                })
                mid_hash += step

            file_db_obj.key_ranges = data_to_data_node.get('nodes_keys')
            file_db_manager.save(file_db_obj)

            logger.info(f"Data to data nodes: {data_to_data_node}")
            logger.info("Sending data to data nodes")
            for data_node in config.data_nodes:
                url = f'http://{data_node["data_node_address"]}/command/shuffle'
                requests.post(url, json=data_to_data_node)


def clear_data(clear_data_request: schemas.ClearDataRequest):
    with session_scope() as session:
        shuffle_db_manager = ShuffleDBManager(session)
        file_db_obj = FileDBManager(session).get_file_by_id(clear_data_request.file_id)

        clear_data_request.folder_name = file_db_obj.file_name

        if clear_data_request.remove_all_data:
            shuffle_db_manager.delete(clear_data_request.file_id)
        else:
            shuffle_db_manager.reset_obj(clear_data_request.file_id)

        return send_request_to_data_nodes(clear_data_request, 'clear_data')
