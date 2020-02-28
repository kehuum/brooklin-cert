import logging
import os
import signal

from xmlrpc.server import SimpleXMLRPCServer

from agent.api.kafka import KafkaCommands
from agent.server.basic import XMLRPCServerBase, XMLRPCBasicServerMixIn
from agent.utils import is_process_running, get_pid_from_file


class XMLRPCKafkaServerMixIn(KafkaCommands):
    """This is the mix-in the provides all the Kafka XML RPC
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
        server.register_function(self.stop_kafka)
        server.register_function(self.kill_kafka)

    # Commands
    def stop_kafka(self):
        pass

    def kill_kafka(self):
        pid = get_pid_from_file('/export/content/lid/apps/kafka/i001/logs/kafka.pid')

        is_running, msg = is_process_running(pid)
        logging.info(f'Kafka pid retrieval status: {msg}')
        if not is_running:
            logging.error(f'Cannot kill Kafka: process {pid} not running')
            raise Exception(f'Kafka process {pid} is not running: {msg}')

        logging.info(f'Killing Kafka with pid: {pid}')
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            logging.error(f'Error when trying to kill Kafka: {e}')
            raise


class XMLRPCKafkaServer(XMLRPCKafkaServerMixIn, XMLRPCBasicServerMixIn, XMLRPCServerBase):
    """This is a Kafka XML RPC server that offers all the functions
    define in agent.api.basic.BasicCommands and agent.api.kafka.KafkaCommands
    """
    pass
