import logging
import subprocess
import time

from abc import ABC, abstractmethod
from collections import namedtuple

DeploymentInfo = namedtuple('DeploymentInfo', ['fabric', 'tag'])


class TestStep(ABC):
    """Base class of all test steps

    It provides two methods, start_time() and end_time(), that capture the test
    step's start and end of execution times in seconds since the epoch.

    Extenders are expected to:
        - Implement run_test() to specify what the test step should do
        - Optionally override cleanup() to add logic to clean up any resources
          created during the test to reset state before the next test
    """

    def __init__(self):
        self.__start_time = None
        self.__end_time = None

    def start_time(self):
        return int(self.__start_time) if self.__start_time else None

    def end_time(self):
        return int(self.__end_time) if self.__end_time else None

    def run(self):
        self.__start_time = time.time()
        self.run_test()
        self.__end_time = time.time()

    def cleanup(self):
        pass

    @abstractmethod
    def run_test(self):
        pass


class RunPythonCommand(TestStep, ABC):
    """Base class of any test step that invovles running a Python script.

    Extenders are expected to:
        - Implement the main_command property to specify the Python script to run
        - Optionally override the cleanup_command property to specify a Python
          script to run whether or not executing main_command failed or not.
    """
    PYTHON_PATH = '/export/apps/python/3.7/bin/python3'

    def run_test(self):
        command = self.main_command
        logging.info(f'Running Python command: {command}')
        RunPythonCommand.run_command(command)

    def cleanup(self):
        command = self.cleanup_command
        if command:
            logging.info(f'Running Python cleanup command: {command}')
            RunPythonCommand.run_command(self.cleanup_command)

    @property
    @abstractmethod
    def main_command(self):
        pass

    @property
    def cleanup_command(self):
        return ''

    @staticmethod
    def run_command(command, timeout=60 * 60 * 15):
        subprocess.run(f'{RunPythonCommand.PYTHON_PATH} {command}', check=True, shell=True, timeout=timeout,
                       universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


class Sleep(TestStep):
    """Test step to sleep"""

    def __init__(self, secs):
        super().__init__()
        if secs <= 0:
            raise ValueError('secs must be greater than zero')
        self._secs = secs

    def run_test(self):
        logging.info(f'Sleeping for {self._secs} seconds')
        time.sleep(self._secs)
