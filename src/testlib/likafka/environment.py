from enum import Enum
from typing import NamedTuple

KAFKA_PRODUCT_NAME = 'kafka-server'

KafkaDeploymentInfo = NamedTuple('KafkaDeploymentInfo',
                                 [('fabric', str), ('tag', str), ('bootstrap_servers', str), ('cc_endpoint', str)])


class KafkaClusterChoice(Enum):
    SOURCE = KafkaDeploymentInfo(fabric='prod-lva1', tag='kafka.cert',
                                 bootstrap_servers='kafka.cert.kafka.prod-lva1.atd.prod.linkedin.com:16637',
                                 cc_endpoint='http://likafka-cruise-control.cert.tag.prod-lva1.atd.prod.linkedin.com:'
                                             '2540/kafkacruisecontrol/')
    DESTINATION = \
        KafkaDeploymentInfo(fabric='prod-lor1', tag='kafka.brooklin-cert',
                            bootstrap_servers='kafka.brooklin-cert.kafka.prod-lor1.atd.prod.linkedin.com:16637',
                            cc_endpoint='http://likafka-cruise-control.brooklin-cert.tag.prod-lor1.atd.prod.linkedin.'
                                        'com:2540/kafkacruisecontrol/')

    @property
    def product_name(self):
        return KAFKA_PRODUCT_NAME
