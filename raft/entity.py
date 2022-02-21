#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from typing import Dict, List, Any

from pydantic import BaseModel
from collections import namedtuple

Address = namedtuple('Address', ['host', 'port'])


class OrchestratorConfig(BaseModel):
    name: str = ''
    address: Address = None
    dbpath: str = ''
    participants: Dict[str, Address] = {}


class NodeMeta(BaseModel):
    name: str
    last_term: int
    last_op_id: int


class Message(BaseModel):
    data_type: str
    data_body: Any
    meta_info: NodeMeta


class HeartBeatMessage(Message):
    data_type = 'heart_beat'


class Oplog(Message):
    data_type = 'oplog'
