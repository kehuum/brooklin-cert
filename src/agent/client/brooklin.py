from xmlrpc.client import ServerProxy
from agent.api.brooklin import BrooklinCommands
from agent.client.basic import XMLRPCBasicClientMixIn, XMLRPCClientBase


class XMLRPCBrooklinClientMixIn(BrooklinCommands):
    """This mix-in the provides all the Brooklin
    client functionality. It cannot be instantiated
    or used on its own, but it can be combined with
    any type that provides the instance method:
        _get_proxy() -> xmlrpc.client.ServerProxy
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__proxy: ServerProxy = self._get_proxy()

    def is_brooklin_leader(self) -> bool:
        proxy = self.__proxy
        return proxy.is_brooklin_leader()

    def pause_brooklin(self):
        proxy = self.__proxy
        proxy.pause_brooklin()

    def resume_brooklin(self):
        proxy = self.__proxy
        proxy.resume_brooklin()

    def start_brooklin(self):
        proxy = self.__proxy
        proxy.start_brooklin()

    def stop_brooklin(self):
        proxy = self.__proxy
        proxy.stop_brooklin()

    def kill_brooklin(self, skip_if_dead=False) -> bool:
        proxy = self.__proxy
        return proxy.kill_brooklin(skip_if_dead)


class XMLRPCBrooklinClient(XMLRPCBrooklinClientMixIn, XMLRPCBasicClientMixIn, XMLRPCClientBase):
    """This is a Brooklin XML RPC client that offers all the functions
    defined in agent.api.basic.BasicCommands and agent.api.brooklin.BrooklinCommands
    """
    pass
