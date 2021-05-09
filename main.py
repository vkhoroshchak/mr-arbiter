from fastapi import FastAPI, HTTPException, status

import schemas
from communication import send_requests
from config.config_provider import config
from config.logger import arbiter_logger
from local_database.utils import FileDBManager

logger = arbiter_logger.get_logger(__name__)

app = FastAPI()


@app.post('/command/create_config_and_filesystem')
async def create_config_and_filesystem(file: schemas.FileSchema):
    db = FileDBManager()
    file_in_db = db.add_new_record(file.file_name)
    logger.info(f"Created file in DB with id {file_in_db.id}")
    send_requests.create_config_and_filesystem(file.file_name)

    return {'distribution': config.distribution, 'file_id': file_in_db.id}


@app.get("/command/check_if_file_is_on_cluster")
def check_if_file_is_on_cluster(file_id: str):
    db = FileDBManager()
    file_in_db = db.get_file_by_id(file_id)

    if file_in_db:
        return {"is_file_on_cluster": True}
    else:
        return {"is_file_on_cluster": False}


@app.post('/command/move_file_to_init_folder')
def move_file_to_init_folder(file_id: str):
    send_requests.send_request_to_data_nodes({"file_id": file_id}, 'move_file_to_init_folder')


@app.post('/command/append')
def append(file: schemas.FileSchema):
    db = FileDBManager()
    file_in_db = db.get_file_by_id(file.file_id)
    data_node_id = 0

    if file_in_db.file_fragments:
        last_data_node_id = int(list(file_in_db.file_fragments[-1].keys())[-1])

        if last_data_node_id != len(config.data_nodes):
            data_node_id = last_data_node_id

    return {'data_node_ip': f'http://{config.data_nodes[data_node_id]["data_node_address"]}'}


@app.post("/command/refresh_table")
def refresh_table(refresh_table_request: schemas.RefreshTableRequest):
    db = FileDBManager()

    file_in_db = db.get_file_by_id(refresh_table_request.file_id)
    data_node_ip = refresh_table_request.ip.split('//')[-1]
    data_node_id = config.get_data_node_id(data_node_ip)

    if data_node_id:
        file_in_db.file_fragments.append(
            {
                data_node_id: refresh_table_request.segment_name
            }
        )
        return db.save(file_in_db)

    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data node ip does not exist"
        )


@app.get("/command/get_file_info", response_model=schemas.FileDBInfo)
def get_file_info(file_id: str):
    db = FileDBManager()
    file_in_db = db.get_file_by_id(file_id)
    return file_in_db