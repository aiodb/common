#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
import asyncio
import os

from entity import OrchestratorConfig, Message
from asyncio import Protocol, get_event_loop
from json import dumps, loads

from states import Follower, State


class Orchestrator:
    """
    Orchestrator presents a node of cluster, switching state of node from follower, candidate and leader.
    Provide tcp server to receive and process messages from client or other nodes of cluster.
    """

    def __init__(self, config: OrchestratorConfig):
        self.name = config.name
        self.participants = config.participants
        os.makedirs(config.dbpath, exist_ok=True)
        loop = get_event_loop()
        self.tcp_server = loop.create_server(lambda: OrchestratorProtocol(self),
                                             host=config.address.host, port=config.address.port)
        self.state = Follower(orchestrator=self)  # start an orchestrator with state as follower

    def run(self):
        loop = get_event_loop()
        loop.run_until_complete(self.tcp_server)
        loop.run_forever()

    def process_received_data(self, message):
        pass

    def send_message_to_target_node(self, node_name: str, message: Message):
        pass

    def broadcast_message(self, message: Message):
        pass

    def change_state(self, new_state):
        self.state.tear_down()
        self.state = new_state(old_state=self.state, orchestrator=self)


class OrchestratorProtocol(Protocol):
    def __init__(self, orchestrator: Orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport: asyncio.Transport):
        print(f'{self.orchestrator.name} tcp server started')
        self.transport = transport

    def data_received(self, data):
        message = loads(data, encoding='utf-8')
        self.orchestrator.process_received_data(self, message)

    def connection_lost(self, exc):
        print('Closed connection with client %s:%s',
              *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(dumps(message))
        self.transport.close()
