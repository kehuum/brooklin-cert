from xmlrpc.client import ServerProxy

from agent.api.kafka import KafkaCommands
from agent.client.basic import XMLRPCBasicClientMixIn, XMLRPCClientBase


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

    def stop_kafka(self):
        pass

    def kill_kafka(self):
        proxy = self.__proxy
        proxy.kill_kafka()


class XMLRPCKafkaClient(XMLRPCKafkaClientMixIn, XMLRPCBasicClientMixIn, XMLRPCClientBase):
    """This is a Kafka XML RPC client that offers all the functions
    defined in agent.api.basic.BasicCommands and agent.api.kafka.KafkaCommands
    """
    pass
