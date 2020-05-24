# Send tasks to datanodes like Map(), Shuffle(), Reduce() etc.
# Send request to mkfile to datanodes
import json
import os.path
import requests

config_data_nodes_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json')

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'json', 'data_nodes.json')) as json_data:
    data = json.load(json_data)

with open(config_data_nodes_path) as data_nodes_file:
    data_nodes_data_json = json.load(data_nodes_file)


class ShuffleManager:
    N = len(data_nodes_data_json['data_nodes'])

    def __init__(self):
        self.counter = 0
        self.list_of_min = []
        self.list_of_max = []

    def min_max_hash(self, context):
        return send_request_to_data_nodes(context, 'min_max_hash')

    def hash(self, context, files_info_dict):
        response = None
        self.list_of_min.append(context['list_keys'][0])
        self.list_of_max.append(context['list_keys'][1])
        self.counter += 1

        if self.counter == ShuffleManager.N:
            max_hash = max(self.list_of_max)
            min_hash = min(self.list_of_min)
            step = (max_hash - min_hash) / ShuffleManager.N

            context = {
                'nodes_keys': [],
                'max_hash': max_hash,
                'file_name': context['file_name'],
                'field_delimiter': context['field_delimiter'],
            }

            mid_hash = min_hash
            self.counter = 0

            for i in data_nodes_data_json['data_nodes']:
                self.counter += 1
                if self.counter == ShuffleManager.N:
                    end_hash = max_hash
                else:
                    end_hash = mid_hash + step
                context['nodes_keys'].append({
                    'data_node_ip': i['data_node_address'],
                    'hash_keys_range': [mid_hash, end_hash]
                })
                mid_hash += step

            for i in files_info_dict['files']:
                # arr = context['file_name'].split('.')
                fn, ext = os.path.splitext(context['file_name'])
                # file_name = arr[0].split('_')[0] + '.' + arr[-1]
                file_name = fn.split("_")[0] + ext
                # if file_name == i['file_name'].split(os.sep)[-1]:
                if file_name == os.path.basename(i["file_name"]):
                    i['key_ranges'] = context['nodes_keys']

            for i in data_nodes_data_json['data_nodes']:
                url = f'http://{i["data_node_address"]}/command/shuffle'
                response = requests.post(url, json=context)
            self.counter = 0

            return response


def create_config_and_filesystem(file_name):
    diction = {'file_name': file_name}
    return send_request_to_data_nodes(diction, 'create_config_and_filesystem')


def map(json_data_obj):
    diction = {
        'mapper': json_data_obj['mapper'],
        'field_delimiter': json_data_obj['field_delimiter'],
        'destination_file': json_data_obj['destination_file'],
    }
    if 'server_source_file' in json_data_obj:
        diction['server_src'] = json_data_obj['server_source_file']
    else:
        diction['source_file'] = json_data_obj['source_file']

    return send_request_to_data_nodes(diction, 'map')


def reduce(json_data_obj):
    diction = {
        'reducer': json_data_obj['reducer'],
        'destination_file': json_data_obj['destination_file'],
        'field_delimiter': json_data_obj['field_delimiter']

    }
    if 'server_source_file' in json_data_obj:
        diction['server_src'] = json_data_obj['server_source_file']
    else:
        diction['source_file'] = json_data_obj['source_file']

    return send_request_to_data_nodes(diction, 'reduce')


def clear_data(context, files_info_dict):
    for item in files_info_dict['files']:
        if item['file_name'] == context['folder_name']:
            files_info_dict['files'].remove(item)
            break
    return send_request_to_data_nodes(context, 'clear_data')


def send_request_to_data_nodes(context, command):
    for item in data['data_nodes']:
        url = f'http://{item["data_node_address"]}/command/{command}'
        response = requests.post(url, json=context)
        response.raise_for_status()
    return response.json()


