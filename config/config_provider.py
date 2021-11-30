import json
from pathlib import Path


class ConfigProvider:
    def __init__(self):
        with open(Path('config', 'data_nodes.json')) as f:
            self.data_nodes_info: dict = json.load(f)

    @property
    def distribution(self):
        return self.data_nodes_info.get('distribution')

    @property
    def data_nodes(self):
        return self.data_nodes_info.get('data_nodes')

    def get_data_node_ip(self, data_node_id: int):
        for data_node in self.data_nodes:
            if data_node.get("data_node_id") == data_node_id:
                return {"data_node_address": data_node.get("data_node_address")}

    def get_data_node_id(self, data_node_ip: int):
        for data_node in self.data_nodes:
            if data_node.get("data_node_address") == data_node_ip:
                return data_node.get("data_node_id")


config = ConfigProvider()
