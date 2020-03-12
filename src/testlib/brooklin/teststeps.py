from collections import namedtuple
from enum import Enum
from testlib.core.teststeps import RunPythonCommand

DeploymentInfo = namedtuple('DeploymentInfo', ['fabric', 'tag'])


class ClusterChoice(Enum):
    CONTROL = DeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.control')
    EXPERIMENT = DeploymentInfo(fabric='prod-lor1', tag='brooklin.cert.candidate')


class CreateDatastream(RunPythonCommand):
    """Test step for creating a datastream"""
    DATASTREAM_CRUD_SCRIPT = 'bmm-datastream.py'

    def __init__(self, cluster=ClusterChoice.CONTROL, name='basic-mirroring-datastream', whitelist='^voyager-api.*',
                 num_tasks=8, topic_create=True, identity=False, passthrough=False, partition_managed=True,
                 cert='identity.p12'):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not whitelist:
            raise ValueError(f'Invalid whitelist: {whitelist}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')

        self.cluster = cluster.value
        self.name = name
        self.whitelist = whitelist
        self.num_tasks = num_tasks
        self.topic_create = topic_create
        self.identity = identity
        self.passthrough = passthrough
        self.partition_managed = partition_managed
        self.cert = cert

    @property
    def main_command(self):
        command = f'{CreateDatastream.DATASTREAM_CRUD_SCRIPT} create ' \
                  f'-n {self.name} ' \
                  f'--whitelist {self.whitelist} ' \
                  f'--numtasks {self.num_tasks} ' \
                  f'--cert {self.cert} ' \
                  f'-f {self.cluster.fabric} -t {self.cluster.tag} ' \
                  '--scd kafka.cert.kafka.prod-lva1.atd.prod.linkedin.com:16637 ' \
                  '--dcd kafka.brooklin-cert.kafka.prod-lor1.atd.prod.linkedin.com:16637 ' \
                  '--applications brooklin-service '

        if self.topic_create:
            command += ' --topiccreate'
        if self.identity:
            command += ' --identity'
        if self.passthrough:
            command += ' --passthrough'
        if self.partition_managed:
            command += ' --partitionmanaged'

        return command

    @property
    def cleanup_command(self):
        return f'{CreateDatastream.DATASTREAM_CRUD_SCRIPT} delete ' \
               f'-n {self.name} ' \
               f'--cert {self.cert} ' \
               f'-f {self.cluster.fabric} -t {self.cluster.tag} ' \
               '--force'
