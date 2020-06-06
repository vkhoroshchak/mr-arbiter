import json
import os
from flask import Flask, request, jsonify

from communication import send_requests

app = Flask(__name__)

config_data_nodes_path = os.path.join(os.path.dirname(__file__), 'config', 'json', 'data_nodes.json')

files_info_dict = {
    'files': []
}
shuffle_manager = send_requests.ShuffleManager()
with open(config_data_nodes_path) as data_nodes_file:
    data_nodes_data_json = json.load(data_nodes_file)


@app.route('/command/create_config_and_filesystem', methods=['POST'])
def create_config_and_filesystem():
    file_name = request.json['file_name']
    send_requests.create_config_and_filesystem(file_name)
    files_info_dict['files'].append(
        {
            'file_name': file_name,
            'lock': False,
            'last_fragment_block_size': 1024,
            'key_ranges': None,
            'file_fragments': []
        })
    global shuffle_manager
    shuffle_manager = send_requests.ShuffleManager()
    return jsonify({'distribution': data_nodes_data_json['distribution']})


@app.route('/command/append', methods=['POST'])
def append():
    file_name = request.json['file_name']
    response = {}

    for item in files_info_dict['files']:
        if item['file_name'] == file_name:

            if len(item['file_fragments']) == 0:
                response['data_node_ip'] = f'http://{data_nodes_data_json["data_nodes"][0]["data_node_address"]}'
            else:

                last_id = int(list(item['file_fragments'][-1].keys())[-1])

                for i in data_nodes_data_json['data_nodes']:
                    if i['data_node_id'] == last_id:

                        if last_id == len(data_nodes_data_json['data_nodes']):

                            response['data_node_ip'] = \
                                f'http://{data_nodes_data_json["data_nodes"][0]["data_node_address"]}'
                        else:
                            response['data_node_ip'] = \
                                f'http://{data_nodes_data_json["data_nodes"][last_id]["data_node_address"]}'
    return jsonify(response)


@app.route("/command/check_if_file_is_on_cluster", methods=["POST"])
def check_if_file_is_on_cluster():
    context = {"is_file_on_cluster": False}
    print("FILES INFO DICT:")
    print(files_info_dict)
    for item in files_info_dict["files"]:
        if item['file_name'] == request.json['file_name']:
            context['is_file_on_cluster'] = True
            break
    return jsonify(context)


@app.route("/command/refresh_table", methods=["POST"])
def refresh_table():

    for item in files_info_dict['files']:
        if item['file_name'] == request.json['file_name']:
            for i in data_nodes_data_json['data_nodes']:

                if i['data_node_address'] == request.json['ip'].split('//')[-1]:
                    data_node_id = i['data_node_id']

                    item['file_fragments'].append(
                        {
                            data_node_id: request.json['segment_name']
                        }
                    )
    return jsonify(success=True)


@app.route("/command/map", methods=["POST"])
def map():
    return send_requests.map(request.json)


@app.route("/command/shuffle", methods=["POST"])
def shuffle():
    shuffle_manager.min_max_hash(request.json)
    return jsonify(success=True)


@app.route("/command/min_max_hash", methods=["POST"])
def min_max_hash():
    shuffle_manager.min_max_hash(request.json)
    return jsonify(success=True)


@app.route("/command/hash", methods=["POST"])
def hash():
    shuffle_manager.hash(request.json, files_info_dict)
    return jsonify(success=True)


@app.route("/command/reduce", methods=["POST"])
def reduce():
    send_requests.reduce(request.json)
    return jsonify(success=True)


@app.route("/command/clear_data", methods=["POST"])
def clear_data():
    send_requests.clear_data(request.json, files_info_dict)
    return jsonify(success=True)


@app.route('/command/get_file', methods=['GET'])
def get_file():
    context = {'data_nodes_ip': []}
    for i in data_nodes_data_json['data_nodes']:
        url = 'http://' + i['data_node_address']
        context['data_nodes_ip'].append(url)

    return jsonify(context)


@app.route('/command/move_file_to_init_folder', methods=['POST'])
def move_file_to_init_folder():
    send_requests.send_request_to_data_nodes(request.json, 'move_file_to_init_folder')

    return jsonify(success=True)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True, use_reloader=False)

