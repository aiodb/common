#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from collections import namedtuple
from typing import Dict, Any

from pydantic import BaseModel

Address = namedtuple('Address', ['host', 'port'])


class OrchestratorConfig(BaseModel):
    name: str = ''
    address: Address = None
    dbpath: str = ''
    participants: Dict[str, Address] = {}


class VoteDetail(BaseModel):
    vote_for: str
    term: int


class NodeMeta(BaseModel):
    name: str
    term: int
    op_id: int


class Message(BaseModel):
    data_type: str
    data_body: Any
    meta_info: NodeMeta


class HeartBeatMessage(Message):
    data_type = 'heart_beat'


class Oplog(Message):
    data_type = 'oplog'


class ElectionMessage(Message):
    data_type = 'election'
