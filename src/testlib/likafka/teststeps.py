from abc import abstractmethod
from enum import Enum

from agent.client.kafka import XMLRPCKafkaClient
from testlib.core.teststeps import RunPythonCommand, TestStep, DeploymentInfo
from testlib.range import get_random_host


class KafkaClusterChoice(Enum):
    SOURCE = DeploymentInfo(fabric='prod-lva1', tag='kafka.cert')
    DESTINATION = DeploymentInfo(fabric='prod-lor1', tag='kafka.brooklin-cert')


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit"""

    def __init__(self, starttime_getter, endtime_getter, topics_file='data/topics.txt'):
        super().__init__()
        if not topics_file:
            raise ValueError(f'Invalid topics file: {topics_file}')

        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.topics_file = topics_file
        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter

    @property
    def main_command(self):
        return 'kafka-audit-v2.py ' \
               f'--topicsfile {self.topics_file} ' \
               f'--startms {self.starttime_getter()} ' \
               f'--endms {self.endtime_getter()}'


class ManipulateKafkaHost(TestStep):
    """Base class for any test step that manipulates the Kafka hosts using the Agent

    Extenders are expected to:
        - Implement the invoke_client_function function to specify the agent client script to run
        - Implement the invoke_client_cleanup_function function if any cleanup steps are required
    """

    def __init__(self, hostname_getter=None):
        super().__init__()
        self.hostname_getter = hostname_getter
        self.host = None

    def run_test(self):
        self.host = self.hostname_getter()
        with XMLRPCKafkaClient(hostname=self.host) as client:
            self.invoke_client_function(client)

    def cleanup(self):
        self.host = self.hostname_getter()
        with XMLRPCKafkaClient(hostname=self.host) as client:
            self.invoke_client_cleanup_function(client)

    def get_host(self):
        return self.host

    @abstractmethod
    def invoke_client_function(self, client):
        pass

    def invoke_client_cleanup_function(self, client):
        pass


class StopRandomKafkaHost(ManipulateKafkaHost):
    """Test step to stop a random Kafka host in the cluster"""

    def __init__(self, cluster):
        super().__init__(hostname_getter=self.get_host)
        self.cluster = cluster.value
        self.host = None

    def invoke_client_function(self, client):
        client.stop_kafka()

    def invoke_client_cleanup_function(self, client):
        client.start_kafka()

    def run_test(self):
        self.host = get_random_host(self.cluster.fabric, self.cluster.tag)
        super().run_test()

    def get_host(self):
        return self.host


class KillRandomKafkaHost(ManipulateKafkaHost):
    """Test step to kill a random Kafka host in the cluster"""

    def __init__(self, cluster):
        super().__init__(hostname_getter=self.get_host)
        self.cluster = cluster.value
        self.host = None

    def invoke_client_function(self, client):
        client.kill_kafka()

    def invoke_client_cleanup_function(self, client):
        client.start_kafka()

    def run_test(self):
        self.host = get_random_host(self.cluster.fabric, self.cluster.tag)
        super().run_test()

    def get_host(self):
        return self.host


class StartKafkaHost(ManipulateKafkaHost):
    """Test step to start a Kafka host"""

    def invoke_client_function(self, client):
        client.start_kafka()
