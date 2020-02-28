import logging
import os
import signal

from xmlrpc.server import SimpleXMLRPCServer

from agent.api.brooklin import BrooklinCommands
from agent.server.basic import XMLRPCServerBase, XMLRPCBasicServerMixIn
from agent.utils import is_process_running


class XMLRPCBrooklinServerMixIn(BrooklinCommands):
    """This is the mix-in the provides all the Brooklin XML RPC
    server functionality. It cannot be instantiated or used
    on its own, but it can be combined with any type that
    provides the instance method:
        _get_server() -> xmlrpc.server.SimpleXMLRPCServer
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__server: SimpleXMLRPCServer = self._get_server()
        self.__register_functions()

    def __register_functions(self):
        server: SimpleXMLRPCServer = self.__server
        server.register_function(self.stop_brooklin)
        server.register_function(self.kill_brooklin)

    # Commands
    def stop_brooklin(self):
        pass

    def kill_brooklin(self):
        pid = XMLRPCBrooklinServerMixIn.get_brooklin_pid()

        is_running, msg = is_process_running(pid)
        logging.info(f'Brooklin pid retrieval status: {msg}')
        if not is_running:
            logging.error(f'Cannot kill Brooklin: process {pid} not running')
            raise Exception(f'Brooklin process {pid} is not running: {msg}')

        logging.info(f'Killing Brooklin with pid: {pid}')
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            logging.error(f'Error when trying to kill Brooklin: {e}')
            raise

    # Static helper functions
    @staticmethod
    def get_brooklin_pid():
        brooklin_pid_file = '/export/content/lid/apps/brooklin-service/i001/logs/brooklin-service.pid'
        with open(brooklin_pid_file) as pid_file:
            brooklin_pid = pid_file.readline().strip()
            return int(brooklin_pid)


class XMLRPCBrooklinServer(XMLRPCBrooklinServerMixIn, XMLRPCBasicServerMixIn, XMLRPCServerBase):
    """This is a Brooklin XML RPC server that offers all the functions
    define in agent.api.basic.BasicCommands and agent.api.brooklin.BrooklinCommands
    """
    pass