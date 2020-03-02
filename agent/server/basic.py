import logging
import subprocess
from xmlrpc.server import SimpleXMLRPCServer

from agent.api import DEFAULT_ADDRESS, DEFAULT_PORT
from agent.api.basic import BasicCommands
from agent.utils import log_errors


class XMLRPCServerBase(object):
    """This is the base agent that encapsulates the basic
    ingredients of an XML RPC server. It does not register
    any functions to execute but it can be mixed-in with
    other objects that register as many functions as needed
    on the SimpleXMLRPCServer owned by this class.
    """
    def __init__(self, address=DEFAULT_ADDRESS, port=DEFAULT_PORT):
        self.__address = address
        self.__port = port
        self.__server = SimpleXMLRPCServer((address, port), bind_and_activate=False, allow_none=True)

    def __enter__(self):
        self.__server.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.__server.__exit__()

    def register_function(self, fn):
        self.__server.register_function(log_errors(fn))

    def serve(self):
        server, address, port = self.__server, self.__address, self.__port
        class_name = self.__class__.__name__
        logging.info(f"{class_name} listening on {address}:{port}")
        server.server_bind()
        server.server_activate()
        server.serve_forever()


class XMLRPCBasicServerMixIn(BasicCommands):
    """This is the mix-in the provides all the basic XML RPC
    server functionality. It cannot be instantiated or used
    on its own, but it can be combined with any type that
    provides the instance method:
        register_function(Callable)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__register_functions()

    def __register_functions(self):
        self.register_function(self.whatami)

    def whatami(self):
        process = subprocess.run("whatami", stdout=subprocess.PIPE, check=True)
        return process.stdout.decode("utf-8").strip()


class XMLRPCBasicServer(XMLRPCBasicServerMixIn, XMLRPCServerBase):
    """This is a basic XML RPC server that offers all the functions
    define in agent.api.basic.BasicCommands
    """
    pass
