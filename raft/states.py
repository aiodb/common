#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
from orchestrator import Orchestrator


class State:
    """
    State is base of state machine to implement follower, candidate and leader of RAFT.
    """

    def __init__(self, old_state: 'State' = None, orchestrator: Orchestrator = None):
        if old_state:
            self.orchestrator = old_state.orchestrator
        else:
            self.orchestrator = orchestrator


class Follower(State):
    def __init__(self, old_state: 'State' = None, orchestrator: Orchestrator = None):
        super(Follower, self).__init__(old_state=old_state, orchestrator=orchestrator)


class Candidate(Follower):
    pass


class Leader(State):
    pass
