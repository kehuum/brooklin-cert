import logging
import os
import signal

from xmlrpc.server import SimpleXMLRPCServer

from agent.api.kafka import KafkaCommands
from agent.server.basic import XMLRPCServerBase, XMLRPCBasicServerMixIn
from agent.utils import is_process_running, get_pid_from_file, run_command


class XMLRPCKafkaServerMixIn(KafkaCommands):
    """This is the mix-in the provides all the Kafka XML RPC
    server functionality. It cannot be instantiated or used
    on its own, but it can be combined with any type that
    provides the instance method:
        register_function(Callable)
    """

    CONTROL_SCRIPT_PATH = '/export/content/lid/apps/kafka/i001/bin/control'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__register_functions()

    def __register_functions(self):
        self.register_function(self.start_kafka)
        self.register_function(self.stop_kafka)
        self.register_function(self.kill_kafka)

    # Commands
    def start_kafka(self):
        command = f'{self.CONTROL_SCRIPT_PATH} start'
        run_command(command)

    def stop_kafka(self):
        command = f'{self.CONTROL_SCRIPT_PATH} stop'
        run_command(command)

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
