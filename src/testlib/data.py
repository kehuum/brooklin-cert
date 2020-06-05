from enum import Enum

DATA_DIR = 'data'


class KafkaTopicFileChoice(Enum):
    VOYAGER = f'{DATA_DIR}/voyager-topics.txt'
    VOYAGER_SEAS = f'{DATA_DIR}/voyager-seas-topics.txt'
    EXPERIMENT_VOYAGER = f'{DATA_DIR}/experiment-voyager-topics.txt'
    EXPERIMENT_VOYAGER_SEAS = f'{DATA_DIR}/experiment-voyager-seas-topics.txt'
