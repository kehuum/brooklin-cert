import uuid

from abc import abstractmethod
from agent.client.brooklin import XMLRPCBrooklinClient
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.core.teststeps import RunPythonCommand, TestStep
from testlib.core.utils import OperationFailedError
from testlib.likafka.teststeps import KafkaClusterChoice
from testlib.range import get_random_host, list_hosts

DATASTREAM_CRUD_SCRIPT = 'bmm-datastream.py'


class CreateDatastream(RunPythonCommand):
    """Test step for creating a datastream"""

    def __init__(self, cluster=BrooklinClusterChoice.CONTROL, name='basic-mirroring-datastream',
                 whitelist='^voyager-api.*', num_tasks=120, topic_create=True, identity=False, passthrough=False,
                 partition_managed=True, cert='identity.p12'):
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


class ManipulateBrooklinHost(TestStep):
    """Base class for any test step that manipulates the Brooklin hosts using the Agent

    Extenders are expected to:
        - Implement the invoke_client_function function to specify the agent client script to run
        - Implement the invoke_client_cleanup_function function if any cleanup steps are required
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
    """Base class for any test step that needs to execute an action on an entire Brooklin cluster

    Extenders are expected to:
        - Implement the process function to execute the action on the specified host
    """

    def __init__(self, cluster: BrooklinClusterChoice):
        super().__init__()
        self.cluster = cluster.value

    def run_test(self):
        for host in list_hosts(fabric=self.cluster.fabric, tag=self.cluster.tag):
            try:
                self.process(host)
            except Exception as err:
                raise OperationFailedError(message=f'Executing cluster action failed on {host}', cause=err)

    @abstractmethod
    def process(self, host):
        pass


class PingBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for pinging the test agents on an entire Brooklin cluster"""

    def process(self, host):
        with XMLRPCBrooklinClient(hostname=host) as client:
            client.ping()


class StopBrooklinHost(ManipulateBrooklinHost):
    """Test step to stop a Brooklin host"""

    def invoke_client_function(self, client):
        client.stop_brooklin()


class StopRandomBrooklinHost(StopBrooklinHost):
    """Test step to stop a random Brooklin host in the cluster"""

    def __init__(self, cluster):
        super().__init__(hostname_getter=self.get_host)
        self.cluster = cluster.value
        self.host = None

    def run_test(self):
        self.host = get_random_host(self.cluster.fabric, self.cluster.tag)
        super().run_test()

    def get_host(self):
        return self.host


class KillBrooklinHost(ManipulateBrooklinHost):
    """Test step to kill a Brooklin host"""

    def __init__(self, skip_if_dead=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip_if_dead = skip_if_dead

    def invoke_client_function(self, client):
        client.kill_brooklin(skip_if_dead=self.skip_if_dead)


class KillRandomBrooklinHost(KillBrooklinHost):
    """Test step to kill a random Brooklin host in the cluster"""

    def __init__(self, cluster):
        super().__init__(hostname_getter=self.get_host)
        self.cluster = cluster.value
        self.host = None

    def run_test(self):
        self.host = get_random_host(self.cluster.fabric, self.cluster.tag)
        super().run_test()

    def get_host(self):
        return self.host


class KillBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for killing Brooklin in an entire cluster"""

    def __init__(self, skip_if_dead=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip_if_dead = skip_if_dead

    def process(self, host):
        KillBrooklinHost(skip_if_dead=self.skip_if_dead, hostname_getter=lambda: host).run()


class StartBrooklinHost(ManipulateBrooklinHost):
    """Test step to start a Brooklin host"""

    def invoke_client_function(self, client):
        client.start_brooklin()


class StartBrooklinCluster(ManipulateBrooklinCluster):
    """Test step for starting Brooklin in an entire cluster"""

    def process(self, host):
        StartBrooklinHost(hostname_getter=lambda: host).run()


class PauseBrooklinHost(ManipulateBrooklinHost):
    """Test step to pause the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.pause_brooklin()


class PauseRandomBrooklinHost(PauseBrooklinHost):
    """Test step to pause the Brooklin process on a random host in the cluster"""

    def __init__(self, cluster):
        super().__init__(hostname_getter=self.get_host)
        self.cluster = cluster.value
        self.host = None

    def run_test(self):
        self.host = get_random_host(self.cluster.fabric, self.cluster.tag)
        super().run_test()

    def get_host(self):
        return self.host


class ResumeBrooklinHost(ManipulateBrooklinHost):
    """Test step to resume the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.resume_brooklin()
