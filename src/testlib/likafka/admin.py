import logging

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from testlib import DEFAULT_CA_FILE


class AdminClient(object):
    def __init__(self, bootstrap_servers, cert_file, key_file):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = self.create_admin_client(bootstrap_servers, cert_file, key_file)

    @staticmethod
    def create_admin_client(bootstrap_servers, cert_file, key_file):
        logging.debug(f'Creating Kafka AdminClient with bootstrap-servers: {bootstrap_servers}, cert file:'
                      f' {cert_file}, key file: {key_file}')
        return KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                security_protocol='SSL',
                                ssl_check_hostname=False,
                                ssl_cafile=DEFAULT_CA_FILE,
                                ssl_certfile=cert_file,
                                ssl_keyfile=key_file)

    def create_topic(self, name, partitions, replication_factor, topic_configs):
        config_dict = dict()
        if topic_configs:
            for config in topic_configs:
                key_value = config.split(':')
                config_dict[key_value[0]] = key_value[1]

        logging.info(f'Creating topic: {name} with partitions: {partitions}, replication factor:'
                     f' {replication_factor}, and topic configs: {config_dict}')
        self.admin_client.create_topics([NewTopic(name=name, num_partitions=partitions,
                                                  replication_factor=replication_factor, topic_configs=config_dict)])

    def delete_topic(self, name):
        logging.info(f'Deleting topic: {name}')
        self.admin_client.delete_topics([name])

    def list_topics(self):
        logging.info(f'Listing topics for bootstrap_servers: {self.bootstrap_servers}')
        topics = self.admin_client.list_topics()
        logging.debug(f'Topics in {self.bootstrap_servers}: {topics}')
        return topics
