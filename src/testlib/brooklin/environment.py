from enum import Enum
from typing import NamedTuple

BROOKLIN_PRODUCT_NAME = 'brooklin-server'
BROOKLIN_SERVICE_NAME = 'brooklin-service'

BrooklinDeploymentInfo = NamedTuple('BrooklinDeploymentInfo',
                                    [('fabric', str), ('tag', str), ('zk_dns', str), ('zk_root_znode', str)])


class BrooklinClusterChoice(Enum):
    CONTROL = BrooklinDeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.control',
                                     zk_dns='zk-lor1-datastream.prod.linkedin.com:12913',
                                     zk_root_znode='/brooklin-cert-control')

    EXPERIMENT = BrooklinDeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.candidate',
                                        zk_dns='zk-lor1-datastream.prod.linkedin.com:12913',
                                        zk_root_znode='/brooklin-cert-candidate')

    @property
    def product_name(self):
        return BROOKLIN_PRODUCT_NAME
