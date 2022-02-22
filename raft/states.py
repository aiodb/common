#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from random import randint

from entity import Message
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
            self.op_id = None
            self.term = 0
            self.vote_for = None

    @classmethod
    def setup(cls):
        print(f'enter state with:{cls.__name__}')

    @classmethod
    def tear_down(cls):
        print(f'leave state:{cls.__name__}')

    @classmethod
    def process_heart_beat_message(cls, message: Message):
        raise NotImplementedError

    @classmethod
    def process_oplog_message(cls, message: Message):
        raise NotImplementedError

    @classmethod
    def process_election_message(cls, message: Message):
        raise NotImplementedError


class Follower(State):
    """
    Node is a follower at its startup.
    """

    def __init__(self, old_state: 'State' = None, orchestrator: 'Orchestrator' = None):
        super(Follower, self).__init__(old_state=old_state, orchestrator=orchestrator)
        self.election_timer = None
        self.setup()

    def setup(self):
        """
        things to do while entering state of follower
        """
        self.restart_election_timer()

    def restart_election_timer(self):
        """
        try to become a candidate
        """
        if hasattr(self, 'election_timer') and self.election_timer:
            self.election_timer.cancle()

        loop = get_event_loop()
        random_delay = randint(10, 20)
        self.election_timer = loop.call_later(random_delay, self.orchestrator.change_state, Candidate)

    @classmethod
    def process_heart_beat_message(cls, message: Message):
        pass

    @classmethod
    def process_oplog_message(cls, message: Message):
        pass

    @classmethod
    def process_election_message(cls, message: Message):
        pass


class Candidate(Follower):
    pass


class Leader(State):
    @classmethod
    def process_heart_beat_message(cls, message: Message):
        pass

    @classmethod
    def process_oplog_message(cls, message: Message):
        pass

    @classmethod
    def process_election_message(cls, message: Message):
        pass
