import requests

from config.config_provider import config


def create_config_and_filesystem(file_name):
    return send_request_to_data_nodes({'file_name': file_name}, 'create_config_and_filesystem')


def send_request_to_data_nodes(context, command):
    for item in config.data_nodes:
        url = f'http://{item["data_node_address"]}/command/{command}'
        requests.post(url, json=context)


def start_map_phase(map_request):
    return send_request_to_data_nodes(map_request.__dict__, 'map')


def start_reduce_phase(reduce_request):
    return send_request_to_data_nodes(reduce_request.__dict__, 'reduce')
