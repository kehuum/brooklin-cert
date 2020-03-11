import logging
import os
import signal

from agent.api.brooklin import BrooklinCommands
from agent.server.basic import XMLRPCServerBase, XMLRPCBasicServerMixIn
from agent.utils import is_process_running, get_pid_from_file, run_command


class XMLRPCBrooklinServerMixIn(BrooklinCommands):
    """This is the mix-in the provides all the Brooklin XML RPC
    server functionality. It cannot be instantiated or used
    on its own, but it can be combined with any type that
    provides the instance method:
        register_function(Callable)
    """

    CONTROL_SCRIPT_PATH = '/export/content/lid/apps/brooklin-service/i001/bin/control'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__register_functions()

    def __register_functions(self):
        self.register_function(self.pause_brooklin)
        self.register_function(self.resume_brooklin)
        self.register_function(self.start_brooklin)
        self.register_function(self.stop_brooklin)
        self.register_function(self.kill_brooklin)

    # Commands
    def pause_brooklin(self):
        logging.info("Trying to pause Brooklin")
        self.send_signal(signal.SIGSTOP)

    def resume_brooklin(self):
        logging.info("Trying to resume Brooklin")
        self.send_signal(signal.SIGCONT)

    def start_brooklin(self):
        command = f'{self.CONTROL_SCRIPT_PATH} start'
        run_command(command)

    def stop_brooklin(self):
        command = f'{self.CONTROL_SCRIPT_PATH} stop'
        run_command(command)

    def kill_brooklin(self):
        logging.info("Trying to kill Brooklin")
        self.send_signal(signal.SIGKILL)

    @staticmethod
    def send_signal(sig):
        pid = get_pid_from_file('/export/content/lid/apps/brooklin-service/i001/logs/brooklin-service.pid')

        is_running, msg = is_process_running(pid)
        logging.info(f'Brooklin pid retrieval status: {msg}')
        if not is_running:
            logging.error(f'Cannot send {sig} signal to Brooklin: process {pid} not running')
            raise Exception(f'Brooklin process {pid} is not running: {msg}')

        logging.info(f'Sending {sig} to Brooklin with pid: {pid}')
        try:
            os.kill(pid, sig)
        except Exception as e:
            logging.error(f'Error when trying to send {sig} to Brooklin: {e}')
            raise


class XMLRPCBrooklinServer(XMLRPCBrooklinServerMixIn, XMLRPCBasicServerMixIn, XMLRPCServerBase):
    """This is a Brooklin XML RPC server that offers all the functions
    define in agent.api.basic.BasicCommands and agent.api.brooklin.BrooklinCommands
    """
    pass
