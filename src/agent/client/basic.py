import logging

from contextlib import AbstractContextManager
from xmlrpc.client import ServerProxy

from agent.api import DEFAULT_PORT
from agent.api.basic import BasicCommands

log = logging.getLogger(__name__)


class XMLRPCClientBase(AbstractContextManager):
    """This is the base class that provides the
    common functionality of all XML RPC clients."""

    def __init__(self, hostname, port=DEFAULT_PORT):
        self.__hostname = hostname
        self.__port = port
        url = f"http://{hostname}:{port}/"
        self.__proxy = ServerProxy(url)
        log.debug(f"Client configured to connect to {url}")

    def __enter__(self):
        self.__proxy.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.__proxy.__exit__()

    def _get_proxy(self):
        return self.__proxy


class XMLRPCBasicClientMixIn(BasicCommands):
    """This mix-in the provides all the basic
    client functionality. It cannot be instantiated
    or used on its own, but it can be combined with
    any type that provides the instance method:
        _get_proxy() -> xmlrpc.client.ServerProxy
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__proxy: ServerProxy = self._get_proxy()

    def whatami(self):
        proxy = self.__proxy
        return proxy.whatami()

    def ping(self):
        proxy = self.__proxy
        return proxy.ping()


class XMLRPCBasicClient(XMLRPCBasicClientMixIn, XMLRPCClientBase):
    """This is a basic XML RPC client that offers all the functions
    defined in agent.api.basic.BasicCommands
    """
    pass
