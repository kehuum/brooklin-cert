"""This file serves two purposes:
    1. It documents how XMLRPCBasicServer/XMLRPCBrooklinServer/XMLRPCKafkaServer can be used
    2. It provides an easy way for us to run XMLRPCBasicServer/XMLRPCBrooklinServer/XMLRPCKafkaServer on the command
       line by simply running: python -m agent.server [--brooklin | --kafka]
"""
import argparse

from agent.api import DEFAULT_ADDRESS, DEFAULT_PORT
from agent.server.basic import XMLRPCBasicServer
from agent.server.brooklin import XMLRPCBrooklinServer
from agent.server.kafka import XMLRPCKafkaServer


def parse_args():
    parser = argparse.ArgumentParser(description='Runs a long running agent to enable remote administration of '
                                                 'Kafka/Brooklin hosts')
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=False)
    mutually_exclusive_group.add_argument('--brooklin', action='store_true', help='Start the XMLRPCBrooklinServer')
    mutually_exclusive_group.add_argument('--kafka', action='store_true', help='Start the XMLRPCKafkaServer')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    address, port = DEFAULT_ADDRESS, DEFAULT_PORT

    if args.brooklin:
        server_type = XMLRPCBrooklinServer
        print(f'Brooklin agent: listening to {address}:{port}')
    elif args.kafka:
        server_type = XMLRPCKafkaServer
        print(f'Kafka agent: listening to {address}:{port}')
    else:
        server_type = XMLRPCBasicServer
        print(f'Basic agent: listening to {address}:{port}')

    with server_type() as server:
        server.serve()
