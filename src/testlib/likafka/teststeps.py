from abc import abstractmethod
from collections import namedtuple
from enum import Enum

from agent.client.kafka import XMLRPCKafkaClient
from testlib import DEFAULT_SSL_CERTFILE
from testlib.core.teststeps import RunPythonCommand, TestStep
from testlib.core.utils import OperationFailedError
from testlib.likafka.admin import AdminClient
from testlib.range import get_random_host

KafkaDeploymentInfo = namedtuple('KafkaDeploymentInfo', ['fabric', 'tag', 'bootstrap_servers'])


class KafkaClusterChoice(Enum):
    SOURCE = KafkaDeploymentInfo(fabric='prod-lva1', tag='kafka.cert',
                                 bootstrap_servers='kafka.cert.kafka.prod-lva1.atd.prod.linkedin.com:16637')
    DESTINATION = \
        KafkaDeploymentInfo(fabric='prod-lor1', tag='kafka.brooklin-cert',
                            bootstrap_servers='kafka.brooklin-cert.kafka.prod-lor1.atd.prod.linkedin.com:16637')


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit"""

    def __init__(self, starttime_getter, endtime_getter, topics_file='data/voyager-topics.txt'):
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

    def __init__(self, hostname_getter):
        super().__init__()
        if not hostname_getter:
            raise ValueError(f'Invalid hostname getter provided: {hostname_getter}')

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


class ValidateSourceAndDestinationTopicsMatch(TestStep):
    """Test step to compare source and destination Kafka topic lists"""

    def __init__(self, source_topics_getter, destination_topics_getter):
        super().__init__()
        if not source_topics_getter or not destination_topics_getter:
            raise ValueError('Both source and destination listed topics getter must be provided')

        self.source_topics_getter = source_topics_getter
        self.destination_topics_getter = destination_topics_getter

    def run_test(self):
        source_topic_set = set(self.source_topics_getter())
        destination_topic_set = set(self.destination_topics_getter())

        if not source_topic_set.issubset(destination_topic_set):
            raise OperationFailedError(f'One or more source topics are not present in the destination: '
                                       f'{", ".join(source_topic_set.difference(destination_topic_set))}')


class ValidateTopicsDoNotExist(TestStep):
    """Test step to validate that a list of topics do not exist"""

    def __init__(self, topics_getter, cluster=KafkaClusterChoice.DESTINATION, ssl_certfile=DEFAULT_SSL_CERTFILE,
                 ssl_keyfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topics_getter:
            raise ValueError(f'Invalid deleted topics getter: {topics_getter}')
        if not cluster:
            raise ValueError(f'Invalid cluster: {cluster}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')
        if not ssl_keyfile:
            raise ValueError(f'Key file must be specified')

        self.topics_getter = topics_getter
        self.cluster = cluster.value
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile

    def run_test(self):
        client = AdminClient([self.cluster.bootstrap_servers], self.ssl_certfile, self.ssl_keyfile)
        current_topics_set = set(client.list_topics())
        deleted_topics_set = set(self.topics_getter())

        if not deleted_topics_set.isdisjoint(current_topics_set):
            raise OperationFailedError(f'Found unexpected topics in Kafka cluster {self.cluster}: '
                                       f'{", ".join(deleted_topics_set.intersection(current_topics_set))}')


class ListTopics(TestStep):
    """Test step for listing topics in a Kafka cluster, optionally filtered by a topic prefix"""

    def __init__(self, bootstrap_servers, topic_prefix_filter='', ssl_certfile=DEFAULT_SSL_CERTFILE,
                 ssl_keyfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not bootstrap_servers:
            raise ValueError(f'Invalid bootstrap_servers: {bootstrap_servers}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')
        if not ssl_keyfile:
            raise ValueError(f'Key file must be specified')

        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix_filter = topic_prefix_filter
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.topics = None

    def run_test(self):
        client = AdminClient(self.bootstrap_servers, self.ssl_certfile, self.ssl_keyfile)
        self.topics = [t for t in client.list_topics() if t.startswith(self.topic_prefix_filter)]

    def get_listed_topics(self):
        return self.topics


class DeleteTopics(TestStep):
    """Test step to delete a list of topics in a Kafka cluster"""

    def __init__(self, topics_getter, bootstrap_servers, ssl_certfile=DEFAULT_SSL_CERTFILE,
                 ssl_keyfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topics_getter:
            raise ValueError(f'Invalid topic topics getter: {topics_getter}')
        if not bootstrap_servers:
            raise ValueError(f'Invalid bootstrap_servers: {bootstrap_servers}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')
        if not ssl_keyfile:
            raise ValueError(f'Key file must be specified')

        self.topics_getter = topics_getter
        self.bootstrap_servers = bootstrap_servers
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile

    def run_test(self):
        client = AdminClient(self.bootstrap_servers, self.ssl_certfile, self.ssl_keyfile)
        topics_to_delete = self.topics_getter()

        # Deleting a single topic at a time because bulk topic deletion needs much longer timeout and may lead
        # to some flakiness
        for topic in topics_to_delete:
            client.delete_topic(topic)
