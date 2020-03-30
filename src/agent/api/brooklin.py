from abc import ABC, abstractmethod


class BrooklinCommands(ABC):
    """This class describes the APIs of commands Brooklin
    clients can request from Brooklin server agents.
    """
    @abstractmethod
    def is_brooklin_leader(self) -> bool:
        pass

    @abstractmethod
    def pause_brooklin(self):
        pass

    @abstractmethod
    def resume_brooklin(self):
        pass

    @abstractmethod
    def start_brooklin(self):
        pass

    @abstractmethod
    def stop_brooklin(self):
        pass

    @abstractmethod
    def kill_brooklin(self, skip_if_dead=False) -> bool:
        pass
