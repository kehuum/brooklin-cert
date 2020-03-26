from collections import namedtuple
from enum import Enum

KAFKA_PRODUCT_NAME = 'kafka-server'

KafkaDeploymentInfo = namedtuple('KafkaDeploymentInfo', ['fabric', 'tag', 'bootstrap_servers', 'cc_endpoint'])


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
