"""This file serves two purposes:
    1. It documents how XMLRPCBasicServer can be used
    2. It provides an easy way for us to run XMLRPCBasicServer on the command line
       by simply running: python -m agent.client --hostname <host>
"""
import argparse

from agent.api import DEFAULT_PORT
from agent.client.basic import XMLRPCBasicClient


def parse_args():
    parser = argparse.ArgumentParser(description='Demonstrates how to use XMLRPCBasicClient')
    parser.add_argument('--hostname', metavar='HOSTNAME', required=True, help='host to connect to')
    parser.add_argument('--port', metavar='PORT', required=False, default=DEFAULT_PORT, help='port to use')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    hostname, port = args.hostname, args.port
    with XMLRPCBasicClient(hostname=hostname, port=port) as client:
        print(f"Invoking whatami on {hostname}:{port}")
        print(client.whatami())
