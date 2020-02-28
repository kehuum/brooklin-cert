"""This file serves two purposes:
    1. It documents how XMLRPCBasicClient/XMLRPCBrooklinClient/XMLRPCKafkaClient can be used
    2. It provides an easy way for us to run XMLRPCBasicClient/XMLRPCBrooklinClient/XMLRPCKafkaClient on the command
       line by simply running: python -m agent.client --hostname <host> [--brooklin | --kafka]
"""
import argparse

from agent.api import DEFAULT_PORT
from agent.client.basic import XMLRPCBasicClient
from agent.client.brooklin import XMLRPCBrooklinClient


def parse_args():
    parser = argparse.ArgumentParser(description='Demonstrates how to use '
                                                 'XMLRPCBasicClient/XMLRPCBrooklinClient/XMLRPCKafkaClient')
    parser.add_argument('--hostname', metavar='HOSTNAME', required=True, help='host to connect to')
    parser.add_argument('--port', metavar='PORT', required=False, default=DEFAULT_PORT, help='port to use')
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=False)
    mutually_exclusive_group.add_argument('--brooklin', action='store_true', help='Start the XMLRPCBrooklinClient')
    mutually_exclusive_group.add_argument('--kafka', action='store_true', help='Start the XMLRPCKafkaClient')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    hostname, port = args.hostname, args.port

    if args.brooklin:
        client_type = XMLRPCBrooklinClient
    elif args.kafka:
        raise NotImplementedError('Invoking XMLRPCKafkaClient is not yet supported')
    else:
        client_type = XMLRPCBasicClient

    with client_type(hostname=hostname, port=port) as client:
        print(f'Invoking whatami on {hostname}:{port}')
        print(client.whatami())
