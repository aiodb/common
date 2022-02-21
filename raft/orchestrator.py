#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
import asyncio
import os

from entity import OrchestratorConfig
from asyncio import Protocol, get_event_loop
from json import dumps, loads


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

    def run(self):
        loop = get_event_loop()
        loop.run_until_complete(self.tcp_server)
        loop.run_forever()

    def data_received_client(self, message):
        pass


class OrchestratorProtocol(Protocol):
    def __init__(self, orchestrator: Orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport: asyncio.Transport):
        print(f'{self.orchestrator.name} tcp server started')
        self.transport = transport

    def data_received(self, data):
        message = loads(data, encoding='utf-8')
        self.orchestrator.data_received_client(self, message)

    def connection_lost(self, exc):
        print('Closed connection with client %s:%s',
              *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(dumps(message))
        self.transport.close()
