import json
import os
from flask import Flask, request, make_response, jsonify
from commands import run_commands
from communication import send_requests

app = Flask(__name__)

config_data_nodes_path = os.path.join(os.path.dirname(__file__), 'config', 'json', 'data_nodes.json')
files_info_path = os.path.join(os.path.dirname(__file__), 'data', 'files_info.json')

with open(config_data_nodes_path) as data_nodes_file:
    data_nodes_data_json = json.load(data_nodes_file)

with open(files_info_path) as files_info_file:
    files_info_file_json = json.load(files_info_file)

with open(files_info_path) as file:
    file_info = json.loads(file.read())


@app.route('/command/make_file', methods=['POST'])
def make_file():
    file_name = request.json['file_name']
    send_requests.make_file(file_name)

    files_info = {
        'files': [
            {

                'file_name': file_name,
                'lock': False,
                'last_fragment_block_size': 1024,
                'key_ranges': None,
                'file_fragments': []
            }
        ]
    }
    with open(files_info_path, 'w+') as file:
        json.dump(files_info, file, indent=4)

    return jsonify({'distribution': data_nodes_data_json['distribution']})


@app.route('/command/append', methods=['POST'])
def append():
    file_name = request.json['file_name']
    response = {}
    for item in files_info_file_json['files']:
        if item['file_name'] == file_name:
            if not item['file_fragments']:
                response['data_node_ip'] = f'http://{data_nodes_data_json["data_nodes"][0]["data_node_address"]}'
            else:
                id = 1

                for key, value in (item['file_fragments'][-1]).items():
                    id = key

                for i in data_nodes_data_json['data_nodes']:
                    if i['data_node_id'] == int(id):
                        prev_ind = data_nodes_data_json['data_nodes'].index(i)
                        if prev_ind + 1 == len(data_nodes_data_json['data_nodes']):
                            response['data_node_ip'] = \
                                f'http://{data_nodes_data_json["data_nodes"][0]["data_node_address"]}'
                        else:
                            response['data_node_ip'] = \
                                f'http://{data_nodes_data_json["data_nodes"][prev_ind + 1]["data_node_address"]}'
    return jsonify(response)


@app.route("/command/refresh_table", methods=["POST"])
def refresh_table():
    for item in file_info['files']:
        if item['file_name'] == request.json['file_name']:
            for i in data_nodes_data_json['data_nodes']:

                if i['data_node_address'] == request.json['ip'].split('//')[-1]:
                    data_node_id = i['data_node_id']
            item['file_fragments'].append(
                {
                    data_node_id: request.json['segment_name']
                }
            )
    with open(files_info_path, 'w') as file:
        json.dump(file_info, file, indent=4)

    return jsonify(success=True)


def map_reduce():
    send_requests.map(request.json['map_reduce'])
    send_requests.reduce(request.json['map_reduce'])


@app.route('/command/get_file', methods=['GET'])
def get_file():
    context = {'data_nodes_ip': []}
    for i in data_nodes_data_json['data_nodes']:
        url = 'http://' + i['data_node_address']
        context['data_nodes_ip'].append(url)

    return jsonify(context)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001)
