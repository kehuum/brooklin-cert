import logging
import re
import uuid

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from testlib import DEFAULT_SSL_CAFILE
from testlib.likafka.environment import KafkaClusterChoice

log = logging.getLogger(__name__)


class AdminClient(object):
    def __init__(self, bootstrap_servers, cert_file):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = self.create_admin_client(bootstrap_servers, cert_file)
        self.consumer_client = self.create_consumer_client(bootstrap_servers, cert_file)

    @staticmethod
    def create_admin_client(bootstrap_servers, ssl_certfile):
        log.debug(f'Creating Kafka AdminClient with bootstrap-servers: {bootstrap_servers}, '
                  f'cert file: {ssl_certfile}')
        return KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                security_protocol='SSL',
                                ssl_check_hostname=False,
                                ssl_cafile=DEFAULT_SSL_CAFILE,
                                ssl_certfile=ssl_certfile,
                                ssl_keyfile=ssl_certfile)

    @staticmethod
    def create_consumer_client(bootstrap_servers, ssl_certfile):
        return KafkaConsumer(group_id=f'{uuid.uuid4()}',
                             auto_offset_reset='earliest',
                             bootstrap_servers=bootstrap_servers,
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile=DEFAULT_SSL_CAFILE,
                             ssl_certfile=ssl_certfile,
                             ssl_keyfile=ssl_certfile)

    def create_topic(self, name, partitions, replication_factor, topic_configs):
        config_dict = dict()
        if topic_configs:
            for config in topic_configs:
                key_value = config.split(':')
                config_dict[key_value[0]] = key_value[1]

        log.info(f'Creating topic: {name} with partitions: {partitions}, replication factor:'
                 f' {replication_factor}, and topic configs: {config_dict}')
        self.admin_client.create_topics([NewTopic(name=name, num_partitions=partitions,
                                                  replication_factor=replication_factor, topic_configs=config_dict)])

    def delete_topic(self, name):
        log.info(f'Deleting topic: {name}')
        self.admin_client.delete_topics([name])

    def list_topics(self):
        log.info(f'Listing topics for bootstrap_servers: {self.bootstrap_servers}')
        topics = self.admin_client.list_topics()
        log.debug(f'Topics in {self.bootstrap_servers}: {topics}')
        return topics

    def get_partition_counts(self, topic_regex):
        log.info(f'Listing topics for bootstrap_servers: {self.bootstrap_servers}')
        topics = self.admin_client.list_topics()
        pattern_matcher = re.compile(topic_regex)
        log.info(f'Total topics: {len(topics)}')
        filtered_topics = [t for t in topics if pattern_matcher.match(t)]
        log.info(f'Filtered topics: {len(filtered_topics)}')
        partition_count = 0
        for topic in filtered_topics:
            partition_count += len(self.consumer_client.partitions_for_topic(topic))
        return partition_count
