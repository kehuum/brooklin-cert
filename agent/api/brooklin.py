from abc import ABC, abstractmethod


class BrooklinCommands(ABC):
    """This class describes the APIs of commands Brooklin
    clients can request from Brooklin server agents.
    """
    @abstractmethod
    def stop_brooklin(self):
        pass

    @abstractmethod
    def kill_brooklin(self):
        pass
