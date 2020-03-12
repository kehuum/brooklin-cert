import logging
import os
import signal
import subprocess

from functools import wraps


def get_pid_from_file(pid_file_name):
    with open(pid_file_name) as pid_file:
        pid = pid_file.read().strip()
        return int(pid)


def is_process_running(pid):
    if pid <= 0:
        raise ValueError(f'Invalid pid: {pid}')
    try:
        # Sending signal 0 does not actually send any signal to the process, but it does invoke error checking
        # such as checking for the existence of the process.
        os.kill(pid, signal.SIG_DFL)
    except PermissionError:
        # Don't have permission to signal this process, but the process exists
        return True, "No permissions to signal the process"
    except ProcessLookupError:
        # Process does not exist
        return False, "Process does not exist"
    else:
        return True, "Process exists and permissions exist to signal the process"


def log_errors(fn):
    @wraps(fn)
    def decorated_fn(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            error_message = f'{fn.__name__} failed with error: {e}\n'
            if isinstance(e, subprocess.CalledProcessError):
                if e.stdout:
                    error_message += f'stdout:\n{e.stdout.decode("utf-8").strip()}\n'
                if e.stderr:
                    error_message += f'stderr:\n{e.stderr.decode("utf-8").strip()}\n'

            logging.error(error_message)
            raise
    return decorated_fn


def run_command(command):
    subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
