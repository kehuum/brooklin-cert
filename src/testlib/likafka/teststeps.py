import random
import re
import string
import uuid

from abc import abstractmethod
from contextlib import AbstractContextManager, nullcontext
from typing import Type
from agent.api.kafka import KafkaCommands
from agent.client.kafka import KafkaDevAgentClient
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from testlib import DEFAULT_SSL_CERTFILE, DEFAULT_SSL_CAFILE
from testlib.core.teststeps import RunPythonCommand, TestStep, Sleep
from testlib.core.utils import OperationFailedError, retry, typename
from testlib.data import KafkaTopicFileChoice
from testlib.likafka.admin import AdminClient
from testlib.likafka.cruisecontrol import CruiseControlClient
from testlib.likafka.environment import KafkaClusterChoice
from testlib.range import get_random_host


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit"""

    def __init__(self, starttime_getter, endtime_getter, topics_file_choice: KafkaTopicFileChoice):
        super().__init__()
        if not topics_file_choice:
            raise ValueError(f'Invalid topics file choice: {topics_file_choice}')

        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.topics_file_choice = topics_file_choice
        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter

    @property
    def main_command(self):
        return 'kafka-audit-v2.py ' \
               f'--topicsfile {self.topics_file_choice.value} ' \
               f'--startms {self.starttime_getter() * 1000} ' \
               f'--endms {self.endtime_getter() * 1000}'

    def __str__(self):
        return f'{typename(self)}(topics_file_choice: {self.topics_file_choice})'


class ManipulateKafkaHost(TestStep):
    """Base class for any test step that manipulates the Kafka hosts using the Agent

    Extenders are expected to:
        - Implement the invoke_client_function function to specify the agent client script to run
        - Implement the invoke_client_cleanup_function function if any cleanup steps are required
    """

    def __init__(self, hostname_getter, client_class: Type[KafkaCommands] = KafkaDevAgentClient):
        super().__init__()
        if not hostname_getter:
            raise ValueError(f'Invalid hostname getter provided: {hostname_getter}')
        if not client_class:
            raise ValueError(f'Invalid client class provided: {client_class}')

        self.hostname_getter = hostname_getter
        self.client_class = client_class
        self.host = None

    def __create_client(self, hostname):
        # If client_class supports context management, use it to create a client directly
        if issubclass(self.client_class, AbstractContextManager):
            return self.client_class(hostname=hostname)  # type: ignore
        # Otherwise, wrap the created client in a null context manager
        return nullcontext(enter_result=self.client_class(hostname=hostname))  # type: ignore

    def run_test(self):
        self.host = self.hostname_getter()
        with self.__create_client(hostname=self.host) as client:
            self.invoke_client_function(client)

    def cleanup(self):
        self.host = self.hostname_getter()
        with self.__create_client(hostname=self.host) as client:  # type: ignore
            self.invoke_client_cleanup_function(client)

    def get_host(self):
        return self.host

    @abstractmethod
    def invoke_client_function(self, client):
        pass

    def invoke_client_cleanup_function(self, client):
        pass

    def __str__(self):
        host = self.get_host() or 'TBD'
        return f'{typename(self)}(host: {host})'


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

    def __init__(self, source_topics_getter, destination_topics_getter, include_all_topics=True):
        super().__init__()
        if not source_topics_getter or not destination_topics_getter:
            raise ValueError('Both source and destination listed topics getter must be provided')

        self.source_topics_getter = source_topics_getter
        self.destination_topics_getter = destination_topics_getter
        self.include_all_topics = include_all_topics

    def run_test(self):
        source_topic_set = set(self.source_topics_getter())
        destination_topic_set = set(self.destination_topics_getter())

        if self.include_all_topics:
            if not source_topic_set.issubset(destination_topic_set):
                raise OperationFailedError(f'One or more source topics are not present in the destination: '
                                           f'{", ".join(source_topic_set.difference(destination_topic_set))}')
        else:
            if not source_topic_set.intersection(destination_topic_set):
                raise OperationFailedError(f'None of the source topics are present in the destination: '
                                           f'{source_topic_set}')


class ValidateTopicsDoNotExist(TestStep):
    """Test step to validate that a list of topics do not exist"""

    # We require specifying one of the predefined Kafka clusters to make
    # sure we never run this step against other Kafka clusters by mistake.
    def __init__(self, topics_getter, cluster=KafkaClusterChoice.DESTINATION, ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topics_getter:
            raise ValueError(f'Invalid deleted topics getter: {topics_getter}')
        if not cluster:
            raise ValueError(f'Invalid cluster: {cluster}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.topics_getter = topics_getter
        self.cluster = cluster.value
        self.ssl_certfile = ssl_certfile

    def run_test(self):
        client = AdminClient([self.cluster.bootstrap_servers], self.ssl_certfile)
        current_topics_set = set(client.list_topics())
        deleted_topics_set = set(self.topics_getter())

        if not deleted_topics_set.isdisjoint(current_topics_set):
            raise OperationFailedError(f'Found unexpected topics in Kafka cluster {self.cluster}: '
                                       f'{", ".join(deleted_topics_set.intersection(current_topics_set))}')


class ValidateDestinationTopicsExist(TestStep):
    """Test step which validates that a list of topics get created on the destination Kafka cluster with retries"""

    def __init__(self, topics, ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topics:
            raise ValueError(f'Invalid topic list: {topics}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.topics = set(topics)
        self.cluster = KafkaClusterChoice.DESTINATION.value
        self.ssl_certfile = ssl_certfile
        self.client = None

    def run_test(self):
        self.client = AdminClient([self.cluster.bootstrap_servers], self.ssl_certfile)
        if not self.topics_exist():
            raise OperationFailedError(f'Not all topics have been created on the destination yet: {self.topics}')

    @retry(tries=12, delay=60)
    def topics_exist(self):
        all_topics = set(self.client.list_topics())
        return self.topics.issubset(all_topics)


class CreateSourceTopic(TestStep):
    """Test step for creating a topic in the source Kafka cluster"""

    def __init__(self, topic_name, partitions=8, replication_factor=3, topic_configs=None,
                 ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topic_name:
            raise ValueError(f'Invalid topic name: {topic_name}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.cluster = KafkaClusterChoice.SOURCE.value
        self.topic_name = topic_name
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.topic_configs = topic_configs
        self.ssl_certfile = ssl_certfile
        self.client = None

    def run_test(self):
        self.client = AdminClient([self.cluster.bootstrap_servers], self.ssl_certfile)
        self.client.create_topic(self.topic_name, self.partitions, self.replication_factor, self.topic_configs)

    def cleanup(self):
        self.client.delete_topic(self.topic_name)

    def __str__(self):
        return f'{typename(self)}(topic: {self.topic_name}, ' \
               f'partitions: {self.partitions}, ' \
               f'replication factor: {self.replication_factor})'


class CreateSourceTopics(TestStep):
    """Test step for creating a list of topics in batches with optional delays"""

    def __init__(self, topics, batch_size=1, delay_seconds=120):
        super().__init__()

        if not topics:
            raise ValueError(f'Invalid topic list: {topics}')
        if batch_size < 1:
            raise ValueError(f'Batch size must be >= 1, invalid batch size: {batch_size}')
        if delay_seconds < 0:
            raise ValueError(f'Delay in seconds must be >= 0, invalid delay: {delay_seconds}')

        self.topics = topics
        self.batch_size = batch_size
        self.delay_seconds = delay_seconds

    def run_test(self):
        for i in range(0, len(self.topics), self.batch_size):
            topic_batch = self.topics[i:i + self.batch_size]

            for topic in topic_batch:
                CreateSourceTopic(topic_name=topic).run()

            if self.delay_seconds > 0:
                Sleep(secs=self.delay_seconds).run()

    def cleanup(self):
        DeleteTopics(topics_getter=self.get_topics, cluster=KafkaClusterChoice.SOURCE).run()

    def get_topics(self):
        return self.topics


class ListTopics(TestStep):
    """Test step for listing topics in a Kafka cluster, optionally filtered by multiple topic prefixes"""

    # We require specifying one of the predefined Kafka clusters to make
    # sure we never run this step against other Kafka clusters by mistake.
    def __init__(self, cluster: KafkaClusterChoice, topic_prefixes_filter=None, ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid Kafka cluster: {cluster}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')
        if topic_prefixes_filter is None:
            topic_prefixes_filter = ['']

        self.cluster = cluster
        self.topic_prefixes_filter = topic_prefixes_filter
        self.ssl_certfile = ssl_certfile
        self.topics = None

    def run_test(self):
        prefixes = '|'.join(re.escape(p) for p in self.topic_prefixes_filter)
        re_prefixes = f'^({prefixes})'
        client = AdminClient([self.cluster.value.bootstrap_servers], self.ssl_certfile)
        self.topics = [t for t in client.list_topics() if re.match(re_prefixes, t)]

    def get_topics(self):
        return self.topics

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'


class DeleteTopics(TestStep):
    """Test step to delete a list of topics in a Kafka cluster"""

    # We require specifying one of the predefined Kafka clusters to make
    # sure we never run this step against other Kafka clusters by mistake.
    def __init__(self, topics_getter, cluster: KafkaClusterChoice, skip_on_failure=False, validate=True,
                 ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topics_getter:
            raise ValueError(f'Invalid topic topics getter: {topics_getter}')
        if not cluster:
            raise ValueError(f'Invalid Kafka cluster: {cluster}')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.topics_getter = topics_getter
        self.cluster = cluster
        self.skip_on_failure = skip_on_failure
        self.validate = validate
        self.ssl_certfile = ssl_certfile

    def run_test(self):
        client = AdminClient([self.cluster.value.bootstrap_servers], self.ssl_certfile)
        topics_to_delete = self.topics_getter()

        # Deleting a single topic at a time because bulk topic deletion needs much longer timeout and may lead
        # to some flakiness
        for topic in topics_to_delete:
            try:
                client.delete_topic(topic)
            except Exception as e:
                if not self.skip_on_failure:
                    raise e

        if self.validate:
            ValidateTopicsDoNotExist(topics_getter=self.topics_getter, cluster=self.cluster).run()

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'


class ConsumeFromDestinationTopic(TestStep):
    """Test step to consume from a topic in the destination Kafka cluster"""

    def __init__(self, topic, num_records, ssl_cafile=DEFAULT_SSL_CAFILE, ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topic:
            raise ValueError(f'Invalid topic: {topic}')
        if num_records < 1:
            raise ValueError(f'Invalid num records: {num_records}')
        if not ssl_cafile:
            raise ValueError(f'SSL CA file must be specified')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.cluster = KafkaClusterChoice.DESTINATION.value
        self.topic = topic
        self.num_records = num_records
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile

    def run_test(self):
        keys_consumed = set()
        consumer = KafkaConsumer(group_id=f'{uuid.uuid4()}',
                                 auto_offset_reset='earliest',
                                 bootstrap_servers=[self.cluster.bootstrap_servers],
                                 security_protocol="SSL",
                                 ssl_check_hostname=False,
                                 ssl_cafile=self.ssl_cafile,
                                 ssl_certfile=self.ssl_certfile,
                                 ssl_keyfile=self.ssl_certfile)

        partitions = consumer.partitions_for_topic(topic=self.topic)

        for partition in partitions:
            tp = TopicPartition(topic=self.topic, partition=partition)

            # Need to know when to stop consuming. Checking the end offset before we start to consume can give us
            # an exit strategy. This means every TopicPartition will need to be consumed one at a time since each
            # will have their own end offsets.
            end_offset = consumer.end_offsets(partitions=[tp])
            beginning_offset = consumer.beginning_offsets(partitions=[tp])

            # Only try to consume if we do have data in the topic
            if end_offset != 0 and end_offset != beginning_offset:
                consumer.assign([tp])
                consumer.seek_to_beginning()
                previous_key = -1
                max_key = -1
                for message in consumer:
                    ConsumeFromDestinationTopic.__validate_record(message=message, keys_consumed=keys_consumed,
                                                                  previous_key=previous_key, max_key=max_key)
                    key = int(message.key)
                    keys_consumed.add(key)
                    if key > max_key:
                        max_key = key
                    previous_key = key
                    if message.offset == end_offset[tp] - 1:
                        break

        sorted_unique_keys = list(sorted(keys_consumed))

        key_count = len(sorted_unique_keys)
        if key_count < self.num_records:
            raise OperationFailedError(f'Expected {self.num_records} records, but found only {key_count} records')

        for i, key in enumerate(sorted_unique_keys):
            if key != i:
                raise OperationFailedError(f'Expected {i} key, but found key {key} instead')

    @staticmethod
    def __validate_record(message, keys_consumed, previous_key, max_key):
        if not message.value:
            raise OperationFailedError(f'Consumed message with empty value: {message}')

        # This will validate that if we see smaller keys than expected, we've seen them before. So this will work
        # for sequences such as [1, 2, 5, 1, 2, 5, 8]
        # This will not work for validating that when a sequence repeats, we get all the elements from that
        # sequence up to the current max seen. Thus, if we skip any keys, we will not be able to identify that they were
        # skipped. Sequences such as [1, 2, 5, 8, 1, 2, 8, 12] will pass, even though 5 was skipped during the replay.
        #
        # An isSublist() function will be need to validate the order every time, and can be quite expensive since
        # it involves maintaining a list of all unique elements seen, seeking to the first element we get out of order
        # in this list, and tracking and validating the full sublist is indeed a sublist of the original list for each
        # new element we get up to the max element seen so far.
        key = int(message.key)
        if key < 0:
            raise OperationFailedError(f'Invalid key found: {key}')
        if key < previous_key and key not in keys_consumed:
            raise OperationFailedError(f'Current key {key} is less than previous key {previous_key} and is not found'
                                       'in keys consumed set')
        if key < max_key and key not in keys_consumed:
            raise OperationFailedError(f'Current key {key} is less than max key {max_key} but was not seen before')

    def __str__(self):
        return f'{typename(self)}(topic: {self.topic})'


class ConsumeFromDestinationTopics(TestStep):
    """Test step to consume from a list of topics in the destination Kafka cluster"""

    def __init__(self, topics, num_records):
        super().__init__()
        if not topics:
            raise ValueError(f'Invalid topic list: {topics}')
        if num_records < 1:
            raise ValueError(f'Invalid num records: {num_records}')

        self.topics = topics
        self.num_records = num_records

    def run_test(self):
        for topic in self.topics:
            ConsumeFromDestinationTopic(topic=topic, num_records=self.num_records).run()


class ProduceToSourceTopic(TestStep):
    """Test step to produce load to a topic in the source Kafka cluster"""

    def __init__(self, topic, num_records=1000, record_size=1000, ssl_cafile=DEFAULT_SSL_CAFILE,
                 ssl_certfile=DEFAULT_SSL_CERTFILE):
        super().__init__()
        if not topic:
            raise ValueError(f'Invalid topic: {topic}')
        if num_records < 1:
            raise ValueError(f'Invalid num records: {num_records}')
        if record_size < 1:
            raise ValueError(f'Invalid record size: {record_size}')
        if not ssl_cafile:
            raise ValueError(f'SSL CA file must be specified')
        if not ssl_certfile:
            raise ValueError(f'Cert file must be specified')

        self.cluster = KafkaClusterChoice.SOURCE.value
        self.topic = topic
        self.num_records = num_records
        self.record_size = record_size
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile

    def run_test(self):
        producer = KafkaProducer(bootstrap_servers=[self.cluster.bootstrap_servers],
                                 security_protocol="SSL",
                                 ssl_check_hostname=False,
                                 ssl_cafile=self.ssl_cafile,
                                 ssl_certfile=self.ssl_certfile,
                                 ssl_keyfile=self.ssl_certfile,
                                 acks=1)

        for i in range(self.num_records):
            payload = str.encode(''.join(random.choices(string.ascii_letters + string.digits, k=self.record_size)))
            key = str.encode(f'{i}')
            producer.send(topic=self.topic, key=key, value=payload)

    def __str__(self):
        return f'{typename(self)}(topic: {self.topic})'


class ProduceToSourceTopics(TestStep):
    """Test step to produce load to a set of topics in the source Kafka cluster"""

    def __init__(self, topics, num_records=1000, record_size=1000):
        super().__init__()
        if not topics:
            raise ValueError(f'Invalid topic list: {topics}')
        if num_records < 1:
            raise ValueError(f'Invalid num records: {num_records}')
        if record_size < 1:
            raise ValueError(f'Invalid record size: {record_size}')

        self.topics = topics
        self.num_records = num_records
        self.record_size = record_size

    def run_test(self):
        for topic in self.topics:
            ProduceToSourceTopic(topic=topic, num_records=self.num_records, record_size=self.record_size).run()


class PerformKafkaPreferredLeaderElection(TestStep):
    """Test step to perform Preferred Leader Election (PLE) on a Kafka cluster"""

    def __init__(self, cluster: KafkaClusterChoice):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid Kafka cluster: {cluster}')

        self.cluster = cluster

    def run_test(self):
        ple = CruiseControlClient(self.cluster.value.cc_endpoint)
        ple.perform_preferred_leader_election()

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'
