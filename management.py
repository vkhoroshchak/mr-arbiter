# TODO: order imports
# TODO: check if **kwargs may be used in recognize_command()

import json
import os
from http import server
from multiprocessing import Process

import requests

from communication import send_requests

# TODO: use context manager
data_nodes_file = open(os.path.join(os.path.dirname(__file__), 'config', 'json', 'data_nodes.json'))
data_nodes_data_json = json.load(data_nodes_file)
# files_info_file = open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json'))
# files_info_file_json = json.load(files_info_file)

# TODO: wrap global bars in class
N = len(data_nodes_data_json['data_nodes'])
counter = 0
list_of_min = []
list_of_max = []


class Handler(server.BaseHTTPRequestHandler):

    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        body_length = int(self.headers['content-length'])
        request_body_json_string = self.rfile.read(body_length).decode('utf-8')

        # Printing  some info to the server console
        print('Server on port ' + str(self.server.server_port) + ' - request body: ' + request_body_json_string)

        json_data_obj = json.loads(request_body_json_string)
        json_data_obj['SEEN_BY_THE_SERVER'] = 'Yes'

        # print(self.request)
        # print(request_body_json_string)
        # print(request_body_json_string["dest_file"])

        # Sending data to the client
        self.wfile.write(bytes(json.dumps(self.recognize_command(json_data_obj)), 'utf-8'))

    def recognize_command(self, content):
        json_data_obj = {}

        if 'make_file' in content:
            json_data_obj = content['make_file']
            send_requests.make_file(json_data_obj["destination_file"])
            file_info = {}
            # TODO: initialize and append to list at the same time
            file_info['files'] = []
            file_info['files'].append(
                {
                    "file_name": json_data_obj["destination_file"],
                    "lock": False,
                    "last_fragment_block_size": 1024,
                    "key_ranges": None,
                    "file_fragments": []
                }
            )
            with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json'), 'w+') as file:
                json.dump(file_info, file, indent=4)

            json_data_obj.clear()
            json_data_obj['distribution'] = data_nodes_data_json['distribution']

        elif 'map_reduce' in content:
            json_data_obj = content['map_reduce']
            send_requests.map(json_data_obj)
            send_requests.reduce(json_data_obj)

        # TODO: refactor append command
        elif 'append' in content:
            json_data_obj = content['append']
            file_name = json_data_obj['file_name']
            json_data_obj = {}
            with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json')) as files_info_file:
                files_info_file_json = json.load(files_info_file)

                for item in files_info_file_json['files']:
                    if item['file_name'] == file_name:
                        if not item['file_fragments']:
                            json_data_obj['data_node_ip'] = 'http://' + data_nodes_data_json['data_nodes'][0][
                                'data_node_address']
                        else:
                            id = 1

                            for key, value in (item['file_fragments'][-1]).items():
                                id = key

                            for i in data_nodes_data_json['data_nodes']:
                                if i['data_node_id'] == int(id):
                                    prev_ind = data_nodes_data_json['data_nodes'].index(i)
                                    if prev_ind + 1 == len(data_nodes_data_json['data_nodes']):
                                        json_data_obj['data_node_ip'] = 'http://' + \
                                                                        data_nodes_data_json['data_nodes'][0][
                                                                            'data_node_address']
                                    else:
                                        json_data_obj['data_node_ip'] = 'http://' + \
                                                                        data_nodes_data_json['data_nodes'][
                                                                            prev_ind + 1][
                                                                            'data_node_address']

            # TODO: check and refactor
        elif 'refresh_table' in content:
            json_data_obj = content['refresh_table']
            with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json')) as file:
                file_info = json.loads(file.read())

            for item in file_info['files']:
                if item['file_name'] == json_data_obj['file_name']:
                    id = ''
                    for i in data_nodes_data_json['data_nodes']:

                        if i['data_node_address'] == json_data_obj['ip'].split('//')[1]:
                            id = i['data_node_id']
                    item['file_fragments'].append(
                        {
                            id: json_data_obj['segment_name']
                        }
                    )
            with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json'), 'w')as file:
                json.dump(file_info, file, indent=4)

        elif 'hash' in content:
            json_data_obj = content['hash']
            list_of_max.append(json_data_obj['list_keys'][0])
            list_of_min.append(json_data_obj['list_keys'][1])

            # TODO: implement through class
            global counter
            global N
            counter += 1

            if counter == N:
                max_hash = max(list_of_max)
                min_hash = min(list_of_min)
                step = (max_hash - min_hash) / N
                context = {
                    'shuffle': {
                        'nodes_keys': [],
                        'max_hash': max_hash,
                        'file_name': json_data_obj['file_name']
                    }
                }
                mid_hash = min_hash
                counter = 0
                for i in data_nodes_data_json['data_nodes']:
                    counter += 1
                    if counter == N:
                        end_hash = max_hash
                    else:
                        end_hash = mid_hash + step
                    context['shuffle']['nodes_keys'].append({
                        'data_node_ip': i['data_node_address'],
                        'hash_keys_range': [mid_hash, end_hash]
                    })
                    mid_hash += step

                with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json')) as files_info_file:
                    files_info_file_json = json.load(files_info_file)

                    for i in files_info_file_json['files']:
                        arr = json_data_obj['file_name'].split('.')
                        file_name = arr[0].split('_')[0] + '.' + arr[-1]
                        if file_name == i['file_name'].split(os.sep)[-1]:
                            print(context['shuffle']['nodes_keys'])
                            i['key_ranges'] = context['shuffle']['nodes_keys']

                    with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json'), 'w')as file:
                        json.dump(files_info_file_json, file, indent=4)

                    for i in data_nodes_data_json['data_nodes']:
                        url = 'http://' + i["data_node_address"]
                        response = requests.post(url, data=json.dumps(context))
                    counter = 0

        elif 'clear_data' in content:
            send_requests.clear_data(content)

        elif 'get_file' in content:
            json_data_obj = content['get_file']
            file_name = json_data_obj['file_name']
            context = {}
            context['data_nodes_ip'] = []
            for i in data_nodes_data_json['data_nodes']:
                url = 'http://' + i["data_node_address"]
                context['data_nodes_ip'].append(url)
            json_data_obj = context

        elif 'get_result_of_key' in content:
            json_data_obj = content['get_result_of_key']
            key = json_data_obj['key']
            file_name = json_data_obj['file_name']
            context = {}
            with open(os.path.join(os.path.dirname(__file__), 'data', 'files_info.json')) as files_info_file:
                files_info_file_json = json.load(files_info_file)
                json_data_obj.clear()

                for item in files_info_file_json['files']:
                    if item['file_name'] == file_name:
                        json_data_obj["key_ranges"] = item["key_ranges"]
                url = 'http://' + data_nodes_data_json['data_nodes'][0]["data_node_address"]
                context['get_hash_of_key'] = key
                response = requests.post(url, data=json.dumps(context))
                json_data_obj['hash_key'] = response.json()

            return json_data_obj


def start_server(server_address):
    my_server = server.ThreadingHTTPServer(server_address, Handler)
    print(str(server_address) + ' Waiting for POST requests...')
    my_server.serve_forever()


def start_local_server_on_port(port):
    p = Process(target=start_server, args=(('127.0.0.1', port),))
    p.start()


if __name__ == '__main__':
    start_local_server_on_port(8011)
