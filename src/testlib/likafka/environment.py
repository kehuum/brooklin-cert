from collections import namedtuple
from enum import Enum

KafkaDeploymentInfo = namedtuple('KafkaDeploymentInfo', ['fabric', 'tag', 'bootstrap_servers'])


class KafkaClusterChoice(Enum):
    SOURCE = KafkaDeploymentInfo(fabric='prod-lva1', tag='kafka.cert',
                                 bootstrap_servers='kafka.cert.kafka.prod-lva1.atd.prod.linkedin.com:16637')
    DESTINATION = \
        KafkaDeploymentInfo(fabric='prod-lor1', tag='kafka.brooklin-cert',
                            bootstrap_servers='kafka.brooklin-cert.kafka.prod-lor1.atd.prod.linkedin.com:16637')
