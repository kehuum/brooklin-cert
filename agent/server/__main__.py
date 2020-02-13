"""This file serves two purposes:
    1. It documents how XMLRPCBasicServer can be used
    2. It provides an easy way for us to run XMLRPCBasicServer on the command line
       by simply running: python -m agent.server
"""
from agent.api import DEFAULT_ADDRESS, DEFAULT_PORT
from agent.server.basic import XMLRPCBasicServer

if __name__ == '__main__':
    with XMLRPCBasicServer() as server:
        address, port = DEFAULT_ADDRESS, DEFAULT_PORT
        print(f"Listening to {address}:{port}")
        server.serve()
