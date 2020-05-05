import logging
import subprocess
import time
import uuid

from abc import ABC, abstractmethod
from typing import Union
from kazoo.client import KazooClient
from testlib import DEFAULT_SSL_CERTFILE, DEFAULT_SSL_CAFILE
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.lid import LidClient
from testlib.likafka.environment import KafkaClusterChoice
from testlib.range import get_random_host


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
        self._id = str(uuid.uuid4())
        self.__start_time = None
        self.__end_time = None

    @property
    def id(self):
        return self._id

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


class ParallelTestStepGroup(object):
    """Represents a collection of TestSteps that are executed in parallel"""

    def __init__(self, *steps: TestStep):
        self._steps = steps

    @property
    def steps(self):
        return self._steps


class RunPythonCommand(TestStep, ABC):
    """Base class of any test step that involves running a Python script.

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
            RunPythonCommand.run_command(command)

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


class RestartCluster(TestStep):
    """Test step to restart a Kafka/Brooklin cluster"""

    def __init__(self, cluster: Union[BrooklinClusterChoice, KafkaClusterChoice], host_concurrency=10,
                 ssl_certfile=DEFAULT_SSL_CERTFILE, ssl_cafile=DEFAULT_SSL_CAFILE):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid Kafka/Brooklin cluster provided: {cluster}')
        if not 0 < host_concurrency <= 100:
            raise ValueError(f'Invalid host concurrency passed: {host_concurrency}. Should be a percentage')
        if not ssl_certfile:
            raise ValueError(f'The SSL certificate path must be provided')
        if not ssl_cafile:
            raise ValueError(f'The SSL CA path must be provided')

        self.cluster = cluster.value
        self.product_name = cluster.product_name
        self.host_concurrency = host_concurrency
        self.ssl_certfile = ssl_certfile
        self.ssl_cafile = ssl_cafile

    def run_test(self):
        lid_client = LidClient(ssl_certfile=self.ssl_certfile, ssl_cafile=self.ssl_cafile)
        lid_client.restart(product=self.product_name, fabric=self.cluster.fabric, product_tag=self.cluster.tag,
                           host_concurrency=self.host_concurrency)


class NukeZooKeeper(TestStep):
    """Deletes everything under the root ZooKeeper znode of a Brooklin cluster

    WARNING: This step must be used with absolute caution since it deletes
             all ZooKeeper nodes under the root znode of the specified Brooklin
             cluster. It is also necessary to make sure all hosts in the
             specified cluster are stopped before running this step.
    """

    def __init__(self, cluster: BrooklinClusterChoice):
        super().__init__()
        self.cluster = cluster.value

    def run_test(self):
        zk_client = KazooClient(hosts=self.cluster.zk_dns)

        try:
            zk_client.start()
            root_znode = self.cluster.zk_root_znode
            for child_znode in zk_client.get_children(root_znode):
                zk_client.delete(f'{root_znode}/{child_znode}', recursive=True)
        finally:
            zk_client.stop()
            zk_client.close()


class GetRandomHostMixIn(object):
    """Mixin to be used with any ManipulateBrooklinHost or ManipulateKafkaHost extender
    that wishes to execute an action against a random host"""

    def __init__(self, cluster: Union[BrooklinClusterChoice, KafkaClusterChoice], **kwargs):
        super().__init__(hostname_getter=lambda: get_random_host(cluster.value.fabric, cluster.value.tag), **kwargs)
