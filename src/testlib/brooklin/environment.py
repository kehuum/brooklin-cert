from collections import namedtuple
from enum import Enum

BROOKLIN_PRODUCT_NAME = 'brooklin-server'

BrooklinDeploymentInfo = namedtuple('BrooklinDeploymentInfo', ['fabric', 'tag', 'zk_dns', 'zk_root_znode'])


class BrooklinClusterChoice(Enum):
    CONTROL = BrooklinDeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.control',
                                     zk_dns='zk-lor1-datastream.prod.linkedin.com:12913',
                                     zk_root_znode='/brooklin-cert-control')

    EXPERIMENT = BrooklinDeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.candidate',
                                        zk_dns='zk-lor1-datastream.prod.linkedin.com:12913',
                                        zk_root_znode='/brooklin-cert-candidate')
