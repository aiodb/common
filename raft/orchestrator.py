#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
import asyncio
import os
from functools import partial

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
        print(f'node:{self.name} with participants:{self.participants}')
        os.makedirs(config.dbpath, exist_ok=True)
        loop = get_event_loop()
        # try to make tcp connection with other nodes of cluster
        self.internal_transport_mapper = {}
        # create tcp server listen on the given port
        self.tcp_server = loop.create_server(lambda: OrchestratorProtocol(self),
                                             host=config.address.host, port=config.address.port)
        self.state = Follower(orchestrator=self)  # start an orchestrator with state as follower

    def run(self):
        loop = get_event_loop()
        # try to make connection with other nodes of cluster
        self.check_and_make_connection_with_peer()
        loop.run_until_complete(self.tcp_server)
        loop.run_forever()

    def check_and_make_connection_with_peer(self):
        loop = get_event_loop()
        loop.call_later(2, self.make_connection_with_peer)

    def make_connection_with_peer(self):
        loop = get_event_loop()
        for node_name, address in self.participants.items():
            if node_name == self.name:
                continue
            if node_name not in self.internal_transport_mapper.keys():
                print(self.internal_transport_mapper)
                try:
                    protocol_factory = partial(InternalProtocol, self, node_name)
                    make_connection = loop.create_connection(protocol_factory, address.host, address.port)
                    asyncio.ensure_future(make_connection)
                except Exception as e:
                    print(e)

        self.check_and_make_connection_with_peer()

    def process_received_client_data(self, message):
        """
        process data received from clients
        """
        pass

    def process_received_peer_data(self, message):
        """
        process data received from other nodes of cluster
        """
        pass

    def send_message_to_target_node(self, node_name: str, message: Message):
        transport = self.internal_transport_mapper.get(node_name)
        if not transport:
            pass
        transport.write(dumps(message.dict()))

    def broadcast_message(self, message: Message):
        """
        send message to all nodes in participants list but self
        """
        for node_name in self.participants.keys():
            self.send_message_to_target_node(node_name, message)

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
        print('data received from clients')
        self.orchestrator.process_received_client_data(self, message)

    def connection_lost(self, exc):
        print('Closed connection with client %s:%s',
              *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(dumps(message))
        self.transport.close()


class InternalProtocol(Protocol):
    def __init__(self, orchestrator: Orchestrator, node_name: str):
        self.node_name = node_name
        self.orchestrator = orchestrator

    def connection_made(self, transport: asyncio.Transport):
        print(f'{self.orchestrator.name} connected to {self.node_name}')
        self.orchestrator.internal_transport_mapper.update({self.node_name: transport})
        self.transport = transport

    def data_received(self, data):
        message = loads(data, encoding='utf-8')
        self.orchestrator.process_received_peer_data(message)

    def connection_lost(self, exc):
        print(f'closed connection with node:{self.node_name}')
        self.orchestrator.internal_transport_mapper.pop(self.node_name, None)

    def send(self, message):
        self.transport.write(dumps(message))
        self.transport.close()
