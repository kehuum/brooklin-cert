import os
import signal


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
