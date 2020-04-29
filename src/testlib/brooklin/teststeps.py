import uuid

from abc import abstractmethod
from functools import partial
from typing import Type
from agent.client.brooklin import XMLRPCBrooklinClient
from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.core.teststeps import RunPythonCommand, TestStep, GetRandomHostMixIn
from testlib.core.utils import OperationFailedError
from testlib.likafka.teststeps import KafkaClusterChoice
from testlib.range import list_hosts

DATASTREAM_CRUD_SCRIPT = 'bmm-datastream.py'


# Datastream steps

class CreateDatastream(RunPythonCommand):
    """Test step for creating a datastream"""

    def __init__(self, datastream_config: DatastreamConfigChoice, name='basic-mirroring-datastream',
                 whitelist='^voyager-api.*', cert='identity.p12'):
        super().__init__()
        if not datastream_config:
            raise ValueError(f'Invalid datastream creation config: {datastream_config}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not whitelist:
            raise ValueError(f'Invalid whitelist: {whitelist}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')

        self.cluster = datastream_config.value.cluster.value
        self.num_tasks = datastream_config.value.num_tasks
        self.topic_create = datastream_config.value.topic_create
        self.identity = datastream_config.value.identity
        self.passthrough = datastream_config.value.passthrough
        self.partition_managed = datastream_config.value.partition_managed
        self.name = name
        self.whitelist = whitelist
        self.cert = cert

    @property
    def main_command(self):
        command = f'{DATASTREAM_CRUD_SCRIPT} create ' \
                  f'-n {self.name} ' \
                  f'--whitelist "{self.whitelist}" ' \
                  f'--numtasks {self.num_tasks} ' \
                  f'--cert {self.cert} ' \
                  f'-f {self.cluster.fabric} -t {self.cluster.tag} ' \
                  f'--scd {KafkaClusterChoice.SOURCE.value.bootstrap_servers} ' \
                  f'--dcd {KafkaClusterChoice.DESTINATION.value.bootstrap_servers} ' \
                  f'--applications brooklin-service --metadata group.id:{uuid.uuid4()} '

        if self.topic_create:
            command += ' --topiccreate'
        if self.identity:
            command += ' --identity'
        if self.passthrough:
            command += ' --passthrough'
        if self.partition_managed:
            command += ' --partitionmanaged'

        return command


class RestartDatastream(RunPythonCommand):
    """Test step for restarting a datastream"""

    def __init__(self, cluster=BrooklinClusterChoice.CONTROL, name='test-restart-datastream', cert='identity.p12'):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')

        self.cluster = cluster.value
        self.name = name
        self.cert = cert

    @property
    def main_command(self):
        return f'{DATASTREAM_CRUD_SCRIPT} restart ' \
               f'-n {self.name} ' \
               f'--cert {self.cert} ' \
               f'-f {self.cluster.fabric} -t {self.cluster.tag}'


class UpdateDatastream(RunPythonCommand):
    """Test step for updating an existing datastream"""

    def __init__(self, whitelist, metadata, name, cluster=BrooklinClusterChoice.CONTROL,
                 cert='identity.p12'):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')
        if not metadata:
            raise ValueError(f'At least one metadata property must be specified: {metadata}')

        self.cluster = cluster.value
        self.name = name
        self.cert = cert
        self.metadata = metadata
        self.whitelist = whitelist

    @property
    def main_command(self):
        command = f'{DATASTREAM_CRUD_SCRIPT} update ' \
                  f'-n {self.name} ' \
                  f'--cert {self.cert} ' \
                  f'-f {self.cluster.fabric} -t {self.cluster.tag} ' \
                  f'--force --restart '

        for metadata in self.metadata:
            command += f'--metadata {metadata} '

        if self.whitelist:
            command += f'--newwhitelist "{self.whitelist}" '

        return command


# Base steps

class ManipulateBrooklinHost(TestStep):
    """Base class for any test step that needs to execution an action on a Brooklin host using
     the test agent

    Extenders are expected to:
        - Implement the invoke_client_function function to specify the agent client script to run
    """

    def __init__(self, hostname_getter=None):
        super().__init__()
        self.hostname_getter = hostname_getter
        self.host = None

    def run_test(self):
        self.host = self.hostname_getter()
        with XMLRPCBrooklinClient(hostname=self.host) as client:
            self.invoke_client_function(client)

    def get_host(self):
        return self.host

    @abstractmethod
    def invoke_client_function(self, client):
        pass


class ManipulateBrooklinCluster(TestStep):
    """Base class for any test step that needs to execute an action on all the hosts of an entire
    Brooklin cluster using the test agent

    """

    def __init__(self, cluster: BrooklinClusterChoice, step_class: Type[ManipulateBrooklinHost]):
        super().__init__()
        self.cluster = cluster.value
        self.step_class = step_class

    def run_test(self):
        for host in list_hosts(fabric=self.cluster.fabric, tag=self.cluster.tag):
            host_step = self.step_class(hostname_getter=lambda: host)
            try:
                host_step.run()
            except Exception as err:
                raise OperationFailedError(
                    message=f'Executing cluster action of type {self.step_class} failed on {host}', cause=err)


# Single host steps

class PingBrooklinHost(ManipulateBrooklinHost):
    """Test step for pinging the test agent on a Brooklin host"""

    def invoke_client_function(self, client):
        client.ping()


class StartBrooklinHost(ManipulateBrooklinHost):
    """Test step to start a Brooklin host"""

    def invoke_client_function(self, client):
        client.start_brooklin()


class StopBrooklinHost(ManipulateBrooklinHost):
    """Test step to stop a Brooklin host"""

    def invoke_client_function(self, client):
        client.stop_brooklin()


class StopRandomBrooklinHost(GetRandomHostMixIn, StopBrooklinHost):
    """Test step to stop a random Brooklin host in the cluster"""
    pass


class KillBrooklinHost(ManipulateBrooklinHost):
    """Test step to kill a Brooklin host"""

    def __init__(self, skip_if_dead=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip_if_dead = skip_if_dead

    def invoke_client_function(self, client):
        client.kill_brooklin(skip_if_dead=self.skip_if_dead)


class KillRandomBrooklinHost(GetRandomHostMixIn, KillBrooklinHost):
    """Test step to kill a random Brooklin host in the cluster"""
    pass


class PauseBrooklinHost(ManipulateBrooklinHost):
    """Test step to pause the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.pause_brooklin()


class PauseRandomBrooklinHost(GetRandomHostMixIn, PauseBrooklinHost):
    """Test step to pause the Brooklin process on a random host in the cluster"""
    pass


class ResumeBrooklinHost(ManipulateBrooklinHost):
    """Test step to resume the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.resume_brooklin()


# Whole cluster steps

class GetBrooklinLeaderHost(TestStep):
    """Test step to get the Brooklin Leader Host"""

    def __init__(self, cluster=BrooklinClusterChoice.CONTROL):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')

        self.cluster = cluster.value
        self.leader_host = None

    def run_test(self):
        hosts = list_hosts(self.cluster.fabric, self.cluster.tag)
        leaders = [h for h in hosts if GetBrooklinLeaderHost.is_leader(h)]
        if len(leaders) != 1:
            raise OperationFailedError(f'Expected exactly one leader but found: {leaders}')
        self.leader_host = leaders[0]

    def get_leader_host(self):
        return self.leader_host

    @staticmethod
    def is_leader(host):
        with XMLRPCBrooklinClient(hostname=host) as client:
            return client.is_brooklin_leader()


class PingBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for pinging the test agents on an entire Brooklin cluster"""

    def __init__(self, cluster: BrooklinClusterChoice):
        super().__init__(cluster=cluster, step_class=PingBrooklinHost)


class KillBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for killing Brooklin in an entire cluster"""

    def __init__(self, cluster: BrooklinClusterChoice, skip_if_dead=False):
        kill_brooklin_host = partial(KillBrooklinHost, skip_if_dead=skip_if_dead)
        super().__init__(cluster=cluster, step_class=kill_brooklin_host)  # type: ignore


class StartBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for starting Brooklin in an entire cluster"""

    def __init__(self, cluster: BrooklinClusterChoice):
        super().__init__(cluster=cluster, step_class=StartBrooklinHost)
