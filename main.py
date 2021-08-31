import datetime

from fastapi import FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder

import schemas
from communication import send_requests
from config.config_provider import config
from config.logger import arbiter_logger
from local_database.utils import FileDBManager, ShuffleDBManager

logger = arbiter_logger.get_logger(__name__)

app = FastAPI()


@app.post('/command/create_config_and_filesystem')
async def create_config_and_filesystem(file: schemas.FileSchema):
    file_db_manager = FileDBManager()
    file_id = file_db_manager.add_new_record(file_name=file.file_name, field_delimiter=file.field_delimiter)

    logger.info(f"Created file in DB with id {file_id}")
    await send_requests.create_config_and_filesystem(file.file_name, file_id)

    return {'distribution': config.distribution, 'file_id': file_id}


@app.get("/command/check_if_file_is_on_cluster")
async def check_if_file_is_on_cluster(file_id: str):
    file_db_manager = FileDBManager()
    file_in_db = file_db_manager.get(file_id)

    if file_in_db:
        return {"is_file_on_cluster": True}
    else:
        return {"is_file_on_cluster": False}


@app.post('/command/move_file_to_init_folder')
async def move_file_to_init_folder(file_id: str):
    await send_requests.send_request_to_data_nodes({"file_id": file_id}, 'move_file_to_init_folder')


@app.get('/command/get-data-nodes-list')
async def get_data_nodes_list():
    return [data_node_info["data_node_address"] for data_node_info in config.data_nodes]


@app.post("/command/refresh_table")
async def refresh_table(refresh_table_request: schemas.RefreshTableRequest):
    file_db_manager = FileDBManager()
    file_in_db = file_db_manager.get(refresh_table_request.file_id)
    data_node_ip = refresh_table_request.ip.split('//')[-1]
    data_node_id = config.get_data_node_id(data_node_ip)

    if data_node_id:
        file_in_db["file_fragments"].append(
            {
                data_node_id: refresh_table_request.segment_name
            }
        )
        file_in_db["updated_at"] = datetime.datetime.now().isoformat(),
        res = file_db_manager.save(refresh_table_request.file_id, file_in_db)
        return res

    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data node ip does not exist"
        )


@app.get("/command/get_file_info", response_model=schemas.FileDBInfo)
async def get_file_info(file_id: str):
    file_db_manager = FileDBManager()
    file_in_db = file_db_manager.get(file_id)
    return file_in_db


@app.get('/command/get_file_name')
async def get_file_name(content: dict):
    file_id = content.get('file_id')
    file_db_manager = FileDBManager()
    file_db_obj = file_db_manager.get(file_id)
    file_name = file_db_obj["file_name"]
    # res = await send_requests.get_file({
    #     'file_name': file_name,
    #     'file_id': file_id,
    # })
    # for stream in res:
    #     resp = await stream
    #     print(92, resp)
    return file_name


@app.post("/command/map")
async def start_map_phase(map_request: schemas.StartMapPhaseRequest):
    logger.info(jsonable_encoder(map_request))
    await send_requests.start_map_phase(map_request)


@app.post("/command/shuffle")
async def start_shuffle_phase(shuffle_request: schemas.StartShufflePhaseRequest):
    logger.info(jsonable_encoder(shuffle_request))
    shuffle_db_manager = ShuffleDBManager()
    shuffle_db_manager.add_new_record(shuffle_request.file_id)
    await send_requests.min_max_hash(shuffle_request)


@app.post("/command/hash")
async def generate_hash_ranges(hash_request: schemas.HashRequest):
    await send_requests.generate_hash_ranges(hash_request)


@app.post("/command/reduce")
async def start_reduce_phase(reduce_request: schemas.StartReducePhaseRequest):
    logger.info(jsonable_encoder(reduce_request))
    await send_requests.start_reduce_phase(reduce_request)


@app.post("/command/clear_data")
async def clear_data(clear_data_request: schemas.ClearDataRequest):
    await send_requests.clear_data(clear_data_request)
