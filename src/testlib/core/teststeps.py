import logging
import subprocess
import time
import uuid

from abc import ABC, abstractmethod
from typing import Union
from kazoo.client import KazooClient
from testlib import DEFAULT_SSL_CERTFILE, DEFAULT_SSL_CAFILE
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.core.utils import OperationFailedError, typename
from testlib.lid import LidClient
from testlib.likafka.environment import KafkaClusterChoice
from testlib.range import get_random_host

log = logging.getLogger(__name__)


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

    def __str__(self):
        return typename(self)


class ParallelTestStepGroup(object):
    """Represents a collection of TestSteps that are executed in parallel"""

    def __init__(self, *steps: TestStep):
        self._steps = steps

    @property
    def steps(self):
        return self._steps

    def __str__(self):
        steps = [str(s) for s in self._steps]
        return f'[{", ".join(steps)}]'


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
        log.info(f'Running Python command: {command}')
        RunPythonCommand.run_command(command)

    def cleanup(self):
        command = self.cleanup_command
        if command:
            log.info(f'Running Python cleanup command: {command}')
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
        log.info(f'Sleeping for {self._secs} seconds')
        time.sleep(self._secs)

    def __str__(self):
        return f'{typename(self)}(secs: {self._secs})'


class SleepUntilNthMinute(TestStep):
    """Test step to sleep until the next nth minute after the ten minute boundary. For example, if the nth minute is 7,
    and current time is 10:18, this test step will sleep until 10:27."""

    TEN_MINUTES_IN_SECONDS = 600

    def __init__(self, nth_minute):
        super().__init__()
        if nth_minute < 0 or nth_minute > 9:
            raise ValueError(f'nth_minute must be in the range of 0 to 9 (inclusive) but was {nth_minute}')
        self._nth_minute = nth_minute

    def run_test(self):
        minute_in_s = self._nth_minute * 60
        current_time_s = int(time.time())
        round_down_ten_s = current_time_s - (current_time_s % self.TEN_MINUTES_IN_SECONDS)
        round_up_ten_s = current_time_s - (current_time_s % self.TEN_MINUTES_IN_SECONDS) + self.TEN_MINUTES_IN_SECONDS
        elapsed_since_round_down_ten_s = current_time_s - round_down_ten_s

        if elapsed_since_round_down_ten_s < minute_in_s:
            time_to_sleep_s = minute_in_s - elapsed_since_round_down_ten_s
        else:
            time_to_sleep_s = (round_up_ten_s + minute_in_s) - current_time_s

        Sleep(secs=time_to_sleep_s).run()

    def __str__(self):
        return f'{typename(self)}(minute: {self._nth_minute})'


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

        self.cluster = cluster
        self.product_name = cluster.product_name
        self.host_concurrency = host_concurrency
        self.ssl_certfile = ssl_certfile
        self.ssl_cafile = ssl_cafile

    def run_test(self):
        lid_client = LidClient(ssl_certfile=self.ssl_certfile, ssl_cafile=self.ssl_cafile)
        lid_client.restart(product=self.product_name, fabric=self.cluster.value.fabric,
                           product_tag=self.cluster.value.tag, host_concurrency=self.host_concurrency)

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'


class NukeZooKeeper(TestStep):
    """Deletes everything under the root ZooKeeper znode of a Brooklin cluster

    WARNING: This step must be used with absolute caution since it deletes
             all ZooKeeper nodes under the root znode of the specified Brooklin
             cluster. It is also necessary to make sure all hosts in the
             specified cluster are stopped before running this step.
    """

    def __init__(self, cluster: BrooklinClusterChoice):
        super().__init__()
        self.cluster = cluster

    def run_test(self):
        zk_client = KazooClient(hosts=self.cluster.value.zk_dns)

        try:
            zk_client.start()
            root_znode = self.cluster.value.zk_root_znode
            for child_znode in zk_client.get_children(root_znode):
                zk_client.delete(f'{root_znode}/{child_znode}', recursive=True)
        finally:
            zk_client.stop()
            zk_client.close()

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'


class GetAndValidateNumTasksFromZooKeeper(TestStep):
    """Gets the numTasks for a given datastream from ZooKeeper and validates that it matches the expected number of
    tasks. numTasks is used by brooklin-service for elastic task assignment only."""

    def __init__(self, cluster: BrooklinClusterChoice, datastream_name, expected_num_tasks_getter):
        super().__init__()
        if not datastream_name:
            raise ValueError(f'Invalid datastream name: {datastream_name}')
        if not expected_num_tasks_getter:
            raise ValueError(f'Invalid expected num tasks getter: {expected_num_tasks_getter}')
        self.cluster = cluster
        self.datastream_name = datastream_name
        self.expected_num_tasks_getter = expected_num_tasks_getter

    def run_test(self):
        expected_num_tasks = self.expected_num_tasks_getter()
        zk_client = KazooClient(hosts=self.cluster.value.zk_dns)

        try:
            zk_client.start()
            root_znode = self.cluster.value.zk_root_znode
            num_tasks_znode = f'{root_znode}/dms/{self.datastream_name}/numTasks'
            if not zk_client.exists(num_tasks_znode):
                raise OperationFailedError(f'The numTasks znode in ZooKeeper does not exist!')

            num_tasks, stat = zk_client.get(num_tasks_znode)
            if expected_num_tasks != int(num_tasks.decode("utf-8")):
                raise OperationFailedError(f'The numTasks in ZooKeeper: {int(num_tasks.decode("utf-8"))} does not '
                                           f'match the expected numTasks: {expected_num_tasks}')
        finally:
            zk_client.stop()
            zk_client.close()

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster}, datastream: {self.datastream_name})'


class GetRandomHostMixIn(object):
    """Mixin to be used with any ManipulateBrooklinHost or ManipulateKafkaHost extender
    that wishes to execute an action against a random host"""

    def __init__(self, cluster: Union[BrooklinClusterChoice, KafkaClusterChoice], **kwargs):
        self._cluster = cluster
        super().__init__(hostname_getter=self._get_hostname, **kwargs)
        self._host = None

    def _get_hostname(self):
        if self._host is None:
            self._host = get_random_host(self._cluster.value.fabric, self._cluster.value.tag)
        return self._host
