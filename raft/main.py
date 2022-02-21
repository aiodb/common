#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/21
import click as click

from conf import cluster_config
from orchestrator import Orchestrator


def setup(conf: str, node_name: str) -> Orchestrator:
    config = cluster_config(conf, node_name)

    orchestrator = Orchestrator(config)

    return orchestrator


@click.command()
@click.option("--conf", '-f', type=str, default='./config.yaml', help="config path", required=True)
@click.option("--node", type=str, required=True)
def main(conf: str, node: str):
    orchestrator = setup(conf, node)
    orchestrator.run()


if __name__ == '__main__':
    main()
