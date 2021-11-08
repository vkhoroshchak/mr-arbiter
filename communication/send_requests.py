import asyncio
import datetime
from aiohttp import ClientSession

import schemas
from config.config_provider import config
from config.logger import arbiter_logger
from local_database.utils import FileDBManager, ShuffleDBManager

logger = arbiter_logger.get_logger(__name__)


async def create_config_and_filesystem(file_name, file_id):
    return await send_request_to_data_nodes({'file_name': file_name, 'file_id': file_id},
                                            'create_config_and_filesystem')


async def check_if_file_is_on_cluster(content, data_nodes=config.data_nodes):
    async with ClientSession() as session:
        logger.info("I am in loop_through_data_nodes method!")
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        for data_node in data_nodes:
            url = f'http://{data_node["data_node_address"]}/command/check_if_file_is_on_cluster'
            try:
                async with session.request(url=url,
                                           headers=headers,
                                           json=content,
                                           timeout=10,
                                           method="GET") as resp:
                    response = await resp.json(content_type=None)
                    logger.info(response)
                    if response['is_file_on_data_node']:
                        return {'is_file_on_data_nodes': True}
            except asyncio.exceptions.TimeoutError:
                pass
        return {'is_file_on_data_nodes': False}


async def send_request_to_data_nodes(data_to_data_node, command, data_nodes=config.data_nodes, method="POST"):
    logger.info(f"Send request to data nodes: {data_to_data_node}")
    async with ClientSession() as session:
        async def send_request(ip_address):
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            url = f'http://{ip_address}/command/{command}'  # noqa
            # data_to_data_node["data_nodes"] = data_nodes
            try:
                async with session.request(url=url,
                                           headers=headers,
                                           json=data_to_data_node,
                                           timeout=10,
                                           method=method) as resp:
                    return await resp.json(content_type=None)
            except asyncio.exceptions.TimeoutError:
                pass

        tasks = []
        for data_node in data_nodes:
            tasks.append(asyncio.ensure_future(send_request(data_node["data_node_address"])))

        await asyncio.gather(*tasks)


# def get_file_data_nodes_list(file_id: str):
#     file_db_manager = FileDBManager()
#
#     file_in_db = file_db_manager.get(file_id)
#     if file_in_db:
#         return [{"data_node_address": x} for x in list(file_in_db["file_fragments"].keys())]
#     return []


async def start_map_phase(map_request):
    return await send_request_to_data_nodes(map_request.__dict__, 'map',
                                            data_nodes=FileDBManager().get_list_of_data_nodes_ip_addresses(
                                                map_request.file_id))


async def start_reduce_phase(reduce_request):
    return await send_request_to_data_nodes(reduce_request.__dict__, 'reduce',
                                            data_nodes=FileDBManager().get_list_of_data_nodes_ip_addresses(
                                                reduce_request.file_id))


async def min_max_hash(shuffle_request: schemas.StartShufflePhaseRequest):
    return await send_request_to_data_nodes(shuffle_request.__dict__, 'min_max_hash',
                                            data_nodes=FileDBManager().get_list_of_data_nodes_ip_addresses(
                                                shuffle_request.file_id))


async def generate_hash_ranges(hash_request: schemas.HashRequest):
    logger.info("Received hash key ranges")
    logger.info(f"Min hash value: {hash_request.min_hash_value}")
    logger.info(f"Max hash value: {hash_request.max_hash_value}")

    shuffle_db_manager = ShuffleDBManager()
    shuffle_db_obj = shuffle_db_manager.get(hash_request.file_id)
    shuffle_db_obj["list_of_max_hashes"].append(hash_request.max_hash_value)
    shuffle_db_obj["list_of_min_hashes"].append(hash_request.min_hash_value)
    shuffle_db_obj["data_nodes_processed"] += 1
    shuffle_db_obj["updated_at"] = datetime.datetime.now().isoformat()
    shuffle_db_manager.save(hash_request.file_id, shuffle_db_obj)

    logger.info(f"Data nodes processed: {shuffle_db_obj['data_nodes_processed']}")

    file_db_manager = FileDBManager()
    file_db_obj = file_db_manager.get(hash_request.file_id)
    data_nodes_ip_addresses = file_db_manager.get_list_of_data_nodes_ip_addresses(hash_request.file_id)

    # If we get key hash ranges from all data nodes - start shuffle phase
    if shuffle_db_obj["data_nodes_processed"] == len(data_nodes_ip_addresses):
        logger.info("Received hash key ranges from all data nodes")
        max_hash = max(shuffle_db_obj["list_of_max_hashes"])
        min_hash = min(shuffle_db_obj["list_of_min_hashes"])
        step = (max_hash - min_hash) / len(data_nodes_ip_addresses)

        data_to_data_node = {
            'nodes_keys': [],
            'max_hash': max_hash,
            'file_id': hash_request.file_id,
            'field_delimiter': file_db_obj["field_delimiter"],
        }
        mid_hash = min_hash

        for index, data_node in enumerate(data_nodes_ip_addresses):
            if index == len(data_nodes_ip_addresses):
                end_hash = max_hash
            else:
                end_hash = mid_hash + step
            data_to_data_node['nodes_keys'].append({
                'data_node_ip': data_node['data_node_address'],
                'hash_keys_range': [mid_hash, end_hash]
            })
            mid_hash += step

        file_db_obj["key_ranges"] = data_to_data_node.get('nodes_keys')
        file_db_obj["updated_at"] = datetime.datetime.now().isoformat()
        file_db_manager.save(hash_request.file_id, file_db_obj)

        logger.info(f"Data to data nodes: {data_to_data_node}")
        logger.info("Sending data to data nodes")
        await send_request_to_data_nodes(data_to_data_node, 'shuffle', data_nodes=data_nodes_ip_addresses)


async def clear_data(clear_data_request: schemas.ClearDataRequest):
    shuffle_db_manager = ShuffleDBManager()
    file_db_manager = FileDBManager()
    file_db_obj = file_db_manager.get(clear_data_request.file_id)

    if file_db_obj:
        clear_data_request.folder_name = file_db_obj["file_name"]

        resp = await send_request_to_data_nodes(clear_data_request.__dict__, 'clear_data',
                                                data_nodes=file_db_manager.get_list_of_data_nodes_ip_addresses(
                                                    clear_data_request.file_id))
        logger.info("send_requests 161")
        logger.info(resp)
        logger.info("send_requests 163")
        # if resp == JSONResponse("Clear data request has been received by data node!"):
        if clear_data_request.remove_all_data:
            shuffle_db_manager.delete(clear_data_request.file_id)
            file_db_manager.delete(clear_data_request.file_id)
        else:
            shuffle_db_manager.reset_obj(clear_data_request.file_id)
            file_db_manager.reset_obj(clear_data_request.file_id)
    else:
        return {}
