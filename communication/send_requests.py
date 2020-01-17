# Send tasks to datanodes like Map(), Shuffle(), Reduce() etc.
# Send request to mkfile to datanodes
import json
import os.path
import requests

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json')) as json_data:
    data = json.load(json_data)


def make_file(file_name):
    diction = {'file_name': file_name}

    return send_request_to_data_nodes(diction, 'make_file')


def map(json_data_obj):
    diction = {
        'mapper': json_data_obj['mapper'],
        'field_delimiter': json_data_obj['field_delimiter'],
        'key_delimiter': json_data_obj['key_delimiter'],
        'destination_file': json_data_obj['destination_file']
    }
    if 'server_source_file' in json_data_obj:
        diction['server_src'] = json_data_obj['server_source_file']
    else:
        diction['source_file'] = json_data_obj['source_file']

    return send_request_to_data_nodes(diction, 'map')


def reduce(json_data_obj):
    diction = {
        'reduce': {
            'reducer': json_data_obj['reducer'],
            'key_delimiter': json_data_obj['key_delimiter'],
            'destination_file': json_data_obj['destination_file']
        }
    }
    if 'server_source_file' in json_data_obj:
        diction['reduce']['server_src'] = json_data_obj['server_source_file']
    else:
        diction['reduce']['source_file'] = json_data_obj['source_file']

    return send_request_to_data_nodes(diction)


def clear_data(context):
    return send_request_to_data_nodes(context)


def send_request_to_data_nodes(context, command):
    for item in data['data_nodes']:
        url = f'http://{item["data_node_address"]}/command/{command}'
        response = requests.post(url, json=context)
        response.raise_for_status()
    return response.json()
