from fastapi import FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder

import schemas
from communication import send_requests
from config.config_provider import config
from config.logger import arbiter_logger
from local_database.utils import session_scope, FileDBManager, ShuffleDBManager

logger = arbiter_logger.get_logger(__name__)

app = FastAPI()


@app.post('/command/create_config_and_filesystem')
async def create_config_and_filesystem(file: schemas.FileSchema):
    with session_scope() as session:
        file_db_manager = FileDBManager(session)
        file_in_db = file_db_manager.add_new_record(file_name=file.file_name, field_delimiter=file.field_delimiter)

        logger.info(f"Created file in DB with id {file_in_db.id}")
        send_requests.create_config_and_filesystem(file.file_name)

        return {'distribution': config.distribution, 'file_id': file_in_db.id}


@app.get("/command/check_if_file_is_on_cluster")
async def check_if_file_is_on_cluster(file_id: str):
    with session_scope() as session:
        file_db_manager = FileDBManager(session)
        file_in_db = file_db_manager.get_file_by_id(file_id)

        if file_in_db:
            return {"is_file_on_cluster": True}
        else:
            return {"is_file_on_cluster": False}


@app.post('/command/move_file_to_init_folder')
async def move_file_to_init_folder(file_id: str):
    send_requests.send_request_to_data_nodes({"file_id": file_id}, 'move_file_to_init_folder')


@app.post('/command/append')
async def append(file: schemas.FileSchema):
    with session_scope() as session:
        file_db_manager = FileDBManager(session)
        file_in_db = file_db_manager.get_file_by_id(file.file_id)
        data_node_id = 0

        if file_in_db.file_fragments:
            last_data_node_id = int(list(file_in_db.file_fragments[-1].keys())[-1])

            if last_data_node_id != len(config.data_nodes):
                data_node_id = last_data_node_id

        return {'data_node_ip': f'http://{config.data_nodes[data_node_id]["data_node_address"]}'}


@app.post("/command/refresh_table")
async def refresh_table(refresh_table_request: schemas.RefreshTableRequest):
    with session_scope() as session:
        file_db_manager = FileDBManager(session)
        file_in_db = file_db_manager.get_file_by_id(refresh_table_request.file_id)
        data_node_ip = refresh_table_request.ip.split('//')[-1]
        data_node_id = config.get_data_node_id(data_node_ip)

        if data_node_id:
            file_in_db.file_fragments.append(
                {
                    data_node_id: refresh_table_request.segment_name
                }
            )
            return file_db_manager.save(file_in_db)

        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data node ip does not exist"
            )


@app.get("/command/get_file_info", response_model=schemas.FileDBInfo)
async def get_file_info(file_id: str):
    with session_scope() as session:
        file_db_manager = FileDBManager(session)
        file_in_db = file_db_manager.get_file_by_id(file_id)
        logger.info(file_db_manager.get_list_of_data_nodes_ip_addresses(file_id))
        return file_in_db


@app.post("/command/map")
async def start_map_phase(map_request: schemas.StartMapPhaseRequest):
    logger.info(jsonable_encoder(map_request))
    send_requests.start_map_phase(map_request)


@app.post("/command/shuffle")
async def start_shuffle_phase(shuffle_request: schemas.StartShufflePhaseRequest):
    logger.info(jsonable_encoder(shuffle_request))
    with session_scope() as session:
        shuffle_db_manager = ShuffleDBManager(session)
        shuffle_db_manager.add_new_record(shuffle_request.file_id)
        send_requests.min_max_hash(shuffle_request)


@app.post("/command/hash")
async def generate_hash_ranges(hash_request: schemas.HashRequest):
    send_requests.generate_hash_ranges(hash_request)


@app.post("/command/reduce")
async def start_reduce_phase(reduce_request: schemas.StartReducePhaseRequest):
    logger.info(jsonable_encoder(reduce_request))
    send_requests.start_reduce_phase(reduce_request)


@app.post("/command/clear_data")
def clear_data(clear_data_request: schemas.ClearDataRequest):
    send_requests.clear_data(clear_data_request)
