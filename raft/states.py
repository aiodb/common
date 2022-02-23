#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from random import randint

from entity import Message, ElectionMessage, NodeMeta, ElectionResponseMessage, HeartBeatMessage
from asyncio import get_event_loop


class State:
    """
    State is base of state machine to implement follower, candidate and leader of RAFT.
    """

    def __init__(self, old_state: 'State' = None, orchestrator: 'Orchestrator' = None):
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.op_id = old_state.op_id
            self.term = old_state.term
            self.vote_for = old_state.vote_for
        else:
            self.orchestrator = orchestrator
            self.op_id = 0
            self.term = 0
            self.vote_for = None

    @classmethod
    def setup(cls):
        print(f'enter state with:{cls.__name__}')

    @classmethod
    def tear_down(cls):
        print(f'leave state:{cls.__name__}')

    def process_heart_beat_message(self, message: Message):
        raise NotImplementedError

    def process_oplog_message(self, message: Message):
        raise NotImplementedError

    def process_election_message(self, message: Message):
        raise NotImplementedError

    def process_election_response_message(self, message: Message):
        raise NotImplementedError


class Follower(State):
    """
    Node is a follower at its startup.
    """

    def __init__(self, old_state: 'State' = None, orchestrator: 'Orchestrator' = None):
        super(Follower, self).__init__(old_state=old_state, orchestrator=orchestrator)
        self.election_timer = None
        self.vote_record = {}
        self.setup()

    def setup(self):
        """
        things to do while entering state of follower
        """
        print(f'entering state:{self.__class__}')
        self.restart_election_timer()

    def tear_down(self):
        """
        things to do while leaving state of follower
        :return:
        """
        print(f'leaving state:{self.__class__}')
        self.cancel_election_timer()

    def restart_election_timer(self):
        """
        try to become a candidate
        """
        if hasattr(self, 'election_timer') and self.election_timer:
            self.election_timer.cancel()

        loop = get_event_loop()
        random_delay = randint(10, 20)
        self.election_timer = loop.call_later(random_delay, self.orchestrator.change_state, Candidate)

    def cancel_election_timer(self):
        """
        try to cancel election timer
        :return:
        """
        if hasattr(self, 'election_timer') and self.election_timer:
            print('current election time cancelled')
            self.election_timer.cancel()

    def process_heart_beat_message(self, message: HeartBeatMessage):
        term_from_leader = message.meta_info.term
        if term_from_leader >= self.term:
            self.restart_election_timer()
            self.term = term_from_leader

    def process_oplog_message(self, message: Message):
        pass

    def process_election_message(self, message: ElectionMessage):
        if self.vote_record.get(self.term):
            print(f'follower has voted at term:{self.term}')
            return
        if message.meta_info.term >= self.term:
            self.vote_for = message.meta_info.name
            self.vote_record.update({self.term: True})
            self.restart_election_timer()

            response_for_vote = ElectionResponseMessage(data_body={},
                                                        meta_info=NodeMeta(name=self.orchestrator.name, term=self.term,
                                                                           op_id=self.op_id))
            self.orchestrator.send_message_to_target_node(self.vote_for, response_for_vote)

    def process_election_response_message(self, message: Message):
        pass


class Candidate(Follower):

    def __init__(self, old_state: 'State' = None, orchestrator: 'Orchestrator' = None):
        super(Candidate, self).__init__(old_state=old_state, orchestrator=orchestrator)
        self.vote_for = self.orchestrator.name
        self.vote_count = 1

    def setup(self):
        print(f'entering state:{self.__class__}')
        print('request for vote')
        self.request_for_vote()

    def tear_down(self):
        print(f'leaving state:{self.__class__}')
        pass

    def request_for_vote(self):
        request_vote_message = ElectionMessage(data_body={},
                                               meta_info=NodeMeta(name=self.orchestrator.name, term=self.term,
                                                                  op_id=self.op_id))
        self.orchestrator.broadcast_message(request_vote_message)

    def process_election_response_message(self, message: Message):
        print(
            f'node:{self.orchestrator.name} received vote from {message.meta_info.name} with term:{message.meta_info.term}')
        self.vote_count += 1
        if self.vote_count > (len(self.orchestrator.participants) + 1) / 2:
            # change state to leader
            self.orchestrator.change_state(Leader)

    def process_heart_beat_message(self, message: HeartBeatMessage):
        term_from_leader = message.meta_info.term
        if term_from_leader >= self.term:
            self.orchestrator.change_state(Follower)

    def process_election_message(self, message: ElectionMessage):
        pass


class Leader(State):

    def __init__(self, old_state: 'State' = None, orchestrator: 'Orchestrator' = None):
        super(Leader, self).__init__(old_state=old_state, orchestrator=orchestrator)
        self.term += 1
        self.heart_beat_timer = None
        self.setup()

    def setup(self):
        print(f'entering state:{self.__class__}')
        self.restart_heart_beat_timer()

    def tear_down(self):
        print(f'leaving state:{self.__class__}')
        self.stop_heart_beat_timer()

    def stop_heart_beat_timer(self):
        if hasattr(self, 'heart_beat_timer') and self.heart_beat_timer:
            self.heart_beat_timer.cancel()

    def restart_heart_beat_timer(self):
        """
        heart beat from leader
        """
        if hasattr(self, 'heart_beat_timer') and self.heart_beat_timer:
            self.heart_beat_timer.cancel()
        self.heart_beat()
        loop = get_event_loop()
        random_delay = 1
        self.heart_beat_timer = loop.call_later(random_delay, self.restart_heart_beat_timer)

    def heart_beat(self):
        heart_beat_message = HeartBeatMessage(data_body={},
                                              meta_info=NodeMeta(name=self.orchestrator.name, term=self.term,
                                                                 op_id=self.op_id))
        self.orchestrator.broadcast_message(heart_beat_message)

    def process_heart_beat_message(self, message: Message):
        pass

    def process_oplog_message(self, message: Message):
        pass

    def process_election_message(self, message: Message):
        if message.meta_info.term > self.term:
            self.vote_for = message.meta_info.name
            response_for_vote = ElectionResponseMessage(data_body={},
                                                        meta_info=NodeMeta(name=self.orchestrator.name, term=self.term,
                                                                           op_id=self.op_id))
            self.orchestrator.send_message_to_target_node(self.vote_for, response_for_vote)
            self.orchestrator.change_state(Follower)

    def process_election_response_message(self, message: Message):
        pass
