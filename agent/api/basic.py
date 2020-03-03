from abc import ABC, abstractmethod


class BasicCommands(ABC):
    """This class describes the APIs of general commands
    that Brooklin or Kafka clients can request from their
    server agents.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    def whatami(self):
        """Runs the whatami command on a host"""
        pass

    @abstractmethod
    def ping(self):
        """Returns a dummy response. Useful for testing connectivity"""
        pass
