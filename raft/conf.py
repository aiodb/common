#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from typing import Dict, List

import yaml

from entity import Address, OrchestratorConfig


def parse_yaml(file_path: str) -> List[Dict]:
    with open(file_path, 'r') as f:
        return yaml.load(f, Loader=yaml.CLoader)


def cluster_config(conf_path: str, current_node: str) -> OrchestratorConfig:
    config = parse_yaml(conf_path)
    current_config = OrchestratorConfig()
    for node_info in config:
        name = node_info.get('name')
        host, port = node_info.get('address').split(':')
        address = Address(host, int(port))
        if name == current_node:
            current_config.name = name
            current_config.address = address
            current_config.dbpath = node_info.get('dbpath')
        else:
            current_config.participants.update({name: address})

    return current_config


if __name__ == '__main__':
    t = parse_yaml('./config.yaml')
    print(t)
