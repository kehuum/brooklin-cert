import logging
import os
import re
import signal
import urllib.request

from agent.api.brooklin import BrooklinCommands
from agent.server.basic import XMLRPCServerBase, XMLRPCBasicServerMixIn
from agent.utils import is_process_running, get_pid_from_file, run_command

log = logging.getLogger(__name__)


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
        self.register_function(self.is_brooklin_leader)
        self.register_function(self.pause_brooklin)
        self.register_function(self.resume_brooklin)
        self.register_function(self.start_brooklin)
        self.register_function(self.stop_brooklin)
        self.register_function(self.kill_brooklin)

    # Commands
    def is_brooklin_leader(self) -> bool:
        is_leader_url = 'http://localhost:2428/brooklin-service/jmx/mbean?objectname=metrics%3Aname%3DCoordinator' \
                        '.isLeader '
        response = urllib.request.urlopen(is_leader_url)
        body = response.read().decode('utf-8')

        is_leader = int(re.findall(r'<td align="right" class="mbean_row">(\d)</td>', body)[0])
        if is_leader < 0 or is_leader > 1:
            raise ValueError(f'isLeader parsed from response is incorrect: {is_leader}')
        return bool(is_leader)

    def pause_brooklin(self):
        log.info("Trying to pause Brooklin")
        self.send_signal(signal.SIGSTOP)

    def resume_brooklin(self):
        log.info("Trying to resume Brooklin")
        self.send_signal(signal.SIGCONT)

    def start_brooklin(self):
        command = f'{self.CONTROL_SCRIPT_PATH} start'
        run_command(command)
        return True

    def stop_brooklin(self):
        command = f'{self.CONTROL_SCRIPT_PATH} stop'
        run_command(command)

    def kill_brooklin(self, skip_if_dead=False) -> bool:
        pid = XMLRPCBrooklinServerMixIn.get_pid()

        if skip_if_dead and not is_process_running(pid)[0]:
            log.info(f"Skipped killing Brooklin because PID {pid} is not running and skip_if_dead is True")
            return False

        log.info("Trying to kill Brooklin")
        self.send_signal(signal.SIGKILL)
        return True

    @staticmethod
    def get_pid():
        return get_pid_from_file('/export/content/lid/apps/brooklin-service/i001/logs/brooklin-service.pid')

    @staticmethod
    def send_signal(sig):
        pid = XMLRPCBrooklinServerMixIn.get_pid()

        is_running, msg = is_process_running(pid)
        log.info(f'Brooklin pid retrieval status: {msg}')
        if not is_running:
            log.error(f'Cannot send {sig} signal to Brooklin: process {pid} not running')
            raise Exception(f'Brooklin process {pid} is not running: {msg}')

        log.info(f'Sending {sig} to Brooklin with pid: {pid}')
        try:
            os.kill(pid, sig)
        except Exception:
            log.exception(f'Error when trying to send {sig} to Brooklin')
            raise


class XMLRPCBrooklinServer(XMLRPCBrooklinServerMixIn, XMLRPCBasicServerMixIn, XMLRPCServerBase):
    """This is a Brooklin XML RPC server that offers all the functions
    define in agent.api.basic.BasicCommands and agent.api.brooklin.BrooklinCommands
    """
    pass
