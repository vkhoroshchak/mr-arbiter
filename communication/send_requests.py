# Send tasks to datanodes like Map(), Shuffle(), Reduce() etc.
# Send request to mkfile to datanodes
import json
import os.path
import requests

config_data_nodes_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json')
files_info_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'files_info.json')

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json')) as json_data:
    data = json.load(json_data)

with open(config_data_nodes_path) as data_nodes_file:
    data_nodes_data_json = json.load(data_nodes_file)


class SomeClass:
    N = len(data_nodes_data_json['data_nodes'])
    counter = 0
    list_of_min = []
    list_of_max = []


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
        'reducer': json_data_obj['reducer'],
        'key_delimiter': json_data_obj['key_delimiter'],
        'destination_file': json_data_obj['destination_file']

    }
    if 'server_source_file' in json_data_obj:
        diction['server_src'] = json_data_obj['server_source_file']
    else:
        diction['source_file'] = json_data_obj['source_file']

    return send_request_to_data_nodes(diction, 'reduce')


def clear_data(context):
    with open(files_info_path) as files_info_file:
        files_info_file_json = json.load(files_info_file)

    open(files_info_path, 'w').close()

    for item in files_info_file_json['files']:
        if item['file_name'] == context['folder_name']:
            files_info_file_json['files'].remove(item)

    with open(files_info_path, 'r+') as file:
        json.dump(files_info_file_json, file, indent=4)

    return send_request_to_data_nodes(context, 'clear_data')


def min_max_hash(context):
    return send_request_to_data_nodes(context, 'min_max_hash')


def send_request_to_data_nodes(context, command):
    for item in data['data_nodes']:
        url = f'http://{item["data_node_address"]}/command/{command}'
        response = requests.post(url, json=context)
        response.raise_for_status()
    return response.json()


def hash(context):
    with open(files_info_path) as files_info_file:
        files_info_file_json = json.load(files_info_file)
    print(context)
    SomeClass.list_of_max.append(context['list_keys'][0])
    SomeClass.list_of_min.append(context['list_keys'][1])
    SomeClass.counter += 1

    if SomeClass.counter == SomeClass.N:
        max_hash = max(SomeClass.list_of_max)
        min_hash = min(SomeClass.list_of_min)
        step = (max_hash - min_hash) / SomeClass.N

        context = {
            'nodes_keys': [],
            'max_hash': max_hash,
            'file_name': context['file_name']
        }

        mid_hash = min_hash
        SomeClass.counter = 0

        for i in data_nodes_data_json['data_nodes']:
            SomeClass.counter += 1
            if SomeClass.counter == SomeClass.N:
                end_hash = max_hash
            else:
                end_hash = mid_hash + step
            context['nodes_keys'].append({
                'data_node_ip': i['data_node_address'],
                'hash_keys_range': [mid_hash, end_hash]
            })
            mid_hash += step

        for i in files_info_file_json['files']:
            arr = context['file_name'].split('.')
            file_name = arr[0].split('_')[0] + '.' + arr[-1]
            if file_name == i['file_name'].split(os.sep)[-1]:
                print(context['nodes_keys'])
                i['key_ranges'] = context['nodes_keys']

        with open(files_info_path, 'w')as file:
            json.dump(files_info_file_json, file, indent=4)

        for i in data_nodes_data_json['data_nodes']:
            url = f'http://{i["data_node_address"]}/command/shuffle'
            response = requests.post(url, json=context)
        SomeClass.counter = 0

    return response
