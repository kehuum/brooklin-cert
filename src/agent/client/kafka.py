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
        self.__send_get_request(url=start_kafka_url, error_msg=f'Failed to start kafka on the host {self.hostname}',
                                check_for_success=True)

    def stop_kafka(self):
        self.__send_suid_command(command='stop-kafka',
                                 error_message=f'Failed to stop kafka on the host {self.hostname}')

    def kill_kafka(self):
        self.__send_suid_command(command='kill-kafka',
                                 error_message=f'Failed to kill kafka on the host {self.hostname}')

    def __send_suid_command(self, command, error_message):
        # Get the suid to use
        action_url = self.base_url + command
        response = self.__send_get_request(url=action_url,
                                           error_msg=f'Failed to retrieve suid on the host {self.hostname}')

        # Perform kafka action passing the suid retrieved in the previous step
        action_url += f'/{response.text}'
        # Checking the status message for success is unreliable, and often returns a message saying the action was
        # not performed, even though it was.
        self.__send_get_request(url=action_url, error_msg=error_message)

    def __send_get_request(self, url, error_msg, check_for_success=False):
        response = send_request(send_fn=lambda: requests.get(url), error_message=error_msg)
        if not response.text:
            raise OperationFailedError(error_msg)

        if check_for_success and self.SUCCESS_INDICATOR not in response.text:
            raise OperationFailedError(f'{error_msg} with error {response.text}')

        return response
