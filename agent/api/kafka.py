from abc import ABC, abstractmethod


class KafkaCommands(ABC):
    """This class describes the APIs of commands Kafka
    clients can request from Kafka server agents.
    """
    @abstractmethod
    def stop_kafka(self):
        pass

    @abstractmethod
    def kill_kafka(self):
        pass
