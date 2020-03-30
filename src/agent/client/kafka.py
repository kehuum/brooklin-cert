import requests

from xmlrpc.client import ServerProxy

from agent.api.kafka import KafkaCommands
from agent.client.basic import XMLRPCBasicClientMixIn, XMLRPCClientBase
from testlib.core.utils import OperationFailedError, send_request


class XMLRPCKafkaClientMixIn(KafkaCommands):
    """This mix-in the provides all the Kafka
    client functionality. It cannot be instantiated
    or used on its own, but it can be combined with
    any type that provides the instance method:
        _get_proxy() -> xmlrpc.client.ServerProxy
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__proxy: ServerProxy = self._get_proxy()

    def start_kafka(self):
        proxy = self.__proxy
        proxy.start_kafka()

    def stop_kafka(self):
        proxy = self.__proxy
        proxy.stop_kafka()

    def kill_kafka(self):
        proxy = self.__proxy
        proxy.kill_kafka()


class XMLRPCKafkaClient(XMLRPCKafkaClientMixIn, XMLRPCBasicClientMixIn, XMLRPCClientBase):
    """This is a Kafka XML RPC client that offers all the functions
    defined in agent.api.basic.BasicCommands and agent.api.kafka.KafkaCommands
    """
    pass


class KafkaDevAgentClient(KafkaCommands):
    """Client which hits the kafka-dev-agent running on the Kafka Brokers to allow operations such as start/stop/kill"""

    SUCCESS_INDICATOR = 'successfully'

    def __init__(self, hostname, port=7750):
        super().__init__()
        if not hostname:
            raise ValueError(f'Invalid hostname provided: {hostname}')
        if port < 0:
            raise ValueError(f'Invalid port provided: {port}')

        self.hostname = hostname
        self.base_url = f'http://{hostname}:{port}/'

    def start_kafka(self):
        start_kafka_url = self.base_url + 'start-kafka'
        self.__get_request(url=start_kafka_url, error_msg=f'Failed to start kafka on the host {self.hostname}')

    def stop_kafka(self):
        # TODO: implement this method
        # This either requires the kafka-dev-agent to be extended to accept a stop command, or it needs LidClient to be
        # extended to take a specific hostname
        pass

    def kill_kafka(self):
        # Get the suid to use for killing Kafka
        kill_kafka_url = self.base_url + 'kill-kafka'
        suid = self.__get_request(url=kill_kafka_url,
                                  error_msg=f'Failed to retrieve suid to kill kafka on the host {self.hostname}',
                                  check_for_success=False)

        # Kill kafka passing the suid retrieved in the previous step
        kill_kafka_url += f'/{suid}'
        self.__get_request(url=kill_kafka_url,
                           error_msg=f'Failed to kill kafka on the host {self.hostname}')

    def __get_request(self, url, error_msg, check_for_success=True):
        response = send_request(send_fn=lambda: requests.get(url), error_message=error_msg)
        if not response.text:
            raise OperationFailedError(error_msg)

        if check_for_success and self.SUCCESS_INDICATOR not in response.text:
            raise OperationFailedError(f'{error_msg} with error {response.text}')

        return response
