import math
import uuid

from abc import abstractmethod
from functools import partial
from typing import Type
from agent.client.brooklin import XMLRPCBrooklinClient
from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.core.teststeps import RunPythonCommand, TestStep, GetRandomHostMixIn
from testlib.core.utils import OperationFailedError, typename
from testlib.likafka.environment import KafkaClusterChoice
from testlib.range import list_hosts

DATASTREAM_CRUD_SCRIPT = 'bmm-datastream.py'
PKCS12_SSL_CERTFILE = 'identity.p12'


# Datastream steps

class CreateDatastream(RunPythonCommand):
    """Test step for creating a datastream"""

    def __init__(self, datastream_config: DatastreamConfigChoice, name, offset_reset=None, enable_cleanup=False,
                 cert=PKCS12_SSL_CERTFILE):
        super().__init__()
        if not datastream_config:
            raise ValueError(f'Invalid datastream creation config: {datastream_config}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')
        if offset_reset and offset_reset != 'earliest' and offset_reset != 'latest':
            raise ValueError(f'Invalid offset reset: {offset_reset}')

        self.datastream_config = datastream_config
        self.cluster = datastream_config.value.cluster.value
        self.num_tasks = datastream_config.value.num_tasks
        self.topic_create = datastream_config.value.topic_create
        self.identity = datastream_config.value.identity
        self.passthrough = datastream_config.value.passthrough
        self.partition_managed = datastream_config.value.partition_managed
        self.whitelist = datastream_config.value.whitelist
        self.auditV3 = datastream_config.value.auditV3
        self.name = name
        self.offset_reset = offset_reset
        self.enable_cleanup = enable_cleanup
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

        if self.offset_reset:
            command += f' --offsetreset {self.offset_reset}'
        if self.topic_create:
            command += ' --topiccreate'
        if self.identity:
            command += ' --identity'
        if self.passthrough:
            command += ' --passthrough'
        if self.partition_managed:
            command += ' --partitionmanaged'
        if self.auditV3:
            command += ' --metadata system.source.enableKafkaAuditV3:"true"'

        return command

    @property
    def cleanup_command(self):
        command = ''
        if self.enable_cleanup:
            command = f'{DATASTREAM_CRUD_SCRIPT} delete ' \
                      f'-n {self.name} ' \
                      f'--cert {self.cert} ' \
                      f'-f {self.cluster.fabric} -t {self.cluster.tag} --force'

        return command

    def __str__(self):
        return f'{typename(self)}(datastream_config: {self.datastream_config})'


class CreateDatastreamWithElasticTaskAssignmentEnabled(RunPythonCommand):
    """Test step for creating a datastream with elastic task assignment enabled. Elastic task assignment only
    works for partition-managed datastreams. This test step will throw if partition-managed is not enabled."""

    def __init__(self, datastream_config: DatastreamConfigChoice, name, partition_count_getter, partitions_per_task,
                 fullness_factor_pct, offset_reset=None, enable_cleanup=False, cert=PKCS12_SSL_CERTFILE):
        super().__init__()
        if not datastream_config:
            raise ValueError(f'Invalid datastream creation config: {datastream_config}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not partition_count_getter:
            raise ValueError(f'Invalid partition count getter: {partition_count_getter}')
        if not partitions_per_task:
            raise ValueError(f'Invalid partitions per task: {partitions_per_task}')
        if not fullness_factor_pct > 0 and not fullness_factor_pct <= 100:
            raise ValueError(f'Invalid fullness factor pct: {fullness_factor_pct}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')
        if offset_reset and offset_reset != 'earliest' and offset_reset != 'latest':
            raise ValueError(f'Invalid offset reset: {offset_reset}')
        if datastream_config.value.partition_managed is not True:
            raise ValueError(f'Elastic task assignment can only be enabled if partition_managed is enabled')

        self.datastream_config = datastream_config
        self.cluster = datastream_config.value.cluster.value
        self.num_tasks = datastream_config.value.num_tasks
        self.topic_create = datastream_config.value.topic_create
        self.identity = datastream_config.value.identity
        self.passthrough = datastream_config.value.passthrough
        self.partition_managed = datastream_config.value.partition_managed
        self.whitelist = datastream_config.value.whitelist
        self.auditV3 = datastream_config.value.auditV3
        self.name = name
        self.partition_count_getter = partition_count_getter
        self.partitions_per_task = partitions_per_task
        self.fullness_factor_pct = fullness_factor_pct
        self.offset_reset = offset_reset
        self.enable_cleanup = enable_cleanup
        self.cert = cert
        self.expected_num_tasks = 0

    @property
    def main_command(self):
        total_partitions = self.partition_count_getter()
        allowed_partitions_per_task = 1
        if (self.partitions_per_task * self.fullness_factor_pct) >= 100:
            allowed_partitions_per_task = math.floor((self.partitions_per_task * self.fullness_factor_pct) / 100)
        num_tasks_needed = math.ceil(total_partitions / allowed_partitions_per_task)
        self.expected_num_tasks = num_tasks_needed
        # Set min_tasks to be smaller than the number of tasks needed to force brooklin-server to recalculate and
        # update the number of tasks needed. Also make sure maxTasks is high enough so that brooklin-server does not
        # get capped by maxTasks when assessing how many tasks are needed.
        min_tasks = num_tasks_needed - 1
        max_tasks = num_tasks_needed + 100
        command = f'{DATASTREAM_CRUD_SCRIPT} create ' \
                  f'-n {self.name} ' \
                  f'--whitelist "{self.whitelist}" ' \
                  f'--numtasks {max_tasks} ' \
                  f'--cert {self.cert} ' \
                  f'-f {self.cluster.fabric} -t {self.cluster.tag} ' \
                  f'--scd {KafkaClusterChoice.SOURCE.value.bootstrap_servers} ' \
                  f'--dcd {KafkaClusterChoice.DESTINATION.value.bootstrap_servers} ' \
                  f'--applications brooklin-service --metadata group.id:{uuid.uuid4()} ' \
                  f'--metadata minTasks:{min_tasks} --metadata partitionsPerTask:{self.partitions_per_task} ' \
                  f'--metadata partitionFullnessThresholdPct:{self.fullness_factor_pct} '

        if self.offset_reset:
            command += f' --offsetreset {self.offset_reset}'
        if self.topic_create:
            command += ' --topiccreate'
        if self.identity:
            command += ' --identity'
        if self.passthrough:
            command += ' --passthrough'
        if self.partition_managed:
            command += ' --partitionmanaged'
        if self.auditV3:
            command += ' --metadata system.source.enableKafkaAuditV3:"true"'

        return command

    @property
    def cleanup_command(self):
        command = ''
        if self.enable_cleanup:
            command = f'{DATASTREAM_CRUD_SCRIPT} delete ' \
                      f'-n {self.name} ' \
                      f'--cert {self.cert} ' \
                      f'-f {self.cluster.fabric} -t {self.cluster.tag} --force'

        return command

    def get_expected_num_tasks(self):
        return self.expected_num_tasks

    def __str__(self):
        return f'{typename(self)}(datastream_config: {self.datastream_config})'


class RestartDatastream(RunPythonCommand):
    """Test step for restarting a datastream"""

    def __init__(self, name, cluster=BrooklinClusterChoice.CONTROL, cert=PKCS12_SSL_CERTFILE):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')

        self.cluster = cluster
        self.name = name
        self.cert = cert

    @property
    def main_command(self):
        return f'{DATASTREAM_CRUD_SCRIPT} restart ' \
               f'-n {self.name} ' \
               f'--cert {self.cert} ' \
               f'-f {self.cluster.value.fabric} -t {self.cluster.value.tag}'

    def __str__(self):
        return f'{typename(self)}(name: {self.name}, cluster: {self.cluster})'


class UpdateDatastream(RunPythonCommand):
    """Test step for updating an existing datastream"""

    def __init__(self, whitelist, metadata, name, cluster=BrooklinClusterChoice.CONTROL,
                 cert=PKCS12_SSL_CERTFILE):
        super().__init__()
        if not cluster:
            raise ValueError(f'Invalid cluster choice: {cluster}')
        if not name:
            raise ValueError(f'Invalid name: {name}')
        if not cert:
            raise ValueError(f'Invalid cert: {cert}')
        if not metadata:
            raise ValueError(f'At least one metadata property must be specified: {metadata}')

        self.cluster = cluster
        self.name = name
        self.cert = cert
        self.metadata = metadata
        self.whitelist = whitelist

    @property
    def main_command(self):
        command = f'{DATASTREAM_CRUD_SCRIPT} update ' \
                  f'-n {self.name} ' \
                  f'--cert {self.cert} ' \
                  f'-f {self.cluster.value.fabric} -t {self.cluster.value.tag} ' \
                  f'--force --restart '

        for metadata in self.metadata:
            command += f'--metadata {metadata} '

        if self.whitelist:
            command += f'--newwhitelist "{self.whitelist}" '

        return command

    def __str__(self):
        return f'{typename(self)}(name: {self.name}, cluster: {self.cluster})'


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

    def __str__(self):
        host = self.get_host() or 'TBD'
        return f'{typename(self)}(host: {host})'


class ManipulateBrooklinCluster(TestStep):
    """Base class for any test step that needs to execute an action on all the hosts of an entire
    Brooklin cluster using the test agent

    """

    def __init__(self, cluster: BrooklinClusterChoice, step_class: Type[ManipulateBrooklinHost]):
        super().__init__()
        self.cluster = cluster
        self.step_class = step_class

    def run_test(self):
        for host in list_hosts(fabric=self.cluster.value.fabric, tag=self.cluster.value.tag):
            host_step = self.step_class(hostname_getter=lambda: host)
            try:
                host_step.run()
            except Exception as err:
                message = f'Executing cluster action of type {self.step_class} failed on {host}'
                raise OperationFailedError(message) from err

    def __str__(self):
        return f'{typename(self)}(cluster: {self.cluster})'


# Single host steps

class GetLeaderBrooklinHostMixIn(object):
    """Mixin to be used with any ManipulateBrooklinHost extender
    that wishes to execute an action against the leader host in
    a Brooklin cluster"""

    def __init__(self, cluster: BrooklinClusterChoice, **kwargs):
        self._cluster = cluster
        super().__init__(hostname_getter=self._get_leader_host, **kwargs)

    def _get_leader_host(self):
        return GetLeaderBrooklinHostMixIn.get_leader_host(self._cluster)

    @staticmethod
    def get_leader_host(cluster):
        hosts = list_hosts(cluster.value.fabric, cluster.value.tag)
        leaders = [h for h in hosts if GetLeaderBrooklinHostMixIn.is_leader(h)]
        if len(leaders) != 1:
            raise OperationFailedError(f'Expected exactly one leader but found: {leaders}')
        return leaders[0]

    @staticmethod
    def is_leader(host):
        with XMLRPCBrooklinClient(hostname=host) as client:
            return client.is_brooklin_leader()


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


class StopLeaderBrooklinHost(GetLeaderBrooklinHostMixIn, StopBrooklinHost):
    """Test step to stop the leader host in a Brooklin cluster"""
    pass


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


class KillLeaderBrooklinHost(GetLeaderBrooklinHostMixIn, KillBrooklinHost):
    """Test step to kill the leader host in a Brooklin cluster"""
    pass


class KillRandomBrooklinHost(GetRandomHostMixIn, KillBrooklinHost):
    """Test step to kill a random Brooklin host in the cluster"""
    pass


class PauseBrooklinHost(ManipulateBrooklinHost):
    """Test step to pause the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.pause_brooklin()


class PauseLeaderBrooklinHost(GetLeaderBrooklinHostMixIn, PauseBrooklinHost):
    """Test step to pause the leader host in a Brooklin cluster"""
    pass


class PauseRandomBrooklinHost(GetRandomHostMixIn, PauseBrooklinHost):
    """Test step to pause the Brooklin process on a random host in the cluster"""
    pass


class ResumeBrooklinHost(ManipulateBrooklinHost):
    """Test step to resume the Brooklin process on a host"""

    def invoke_client_function(self, client):
        client.resume_brooklin()


# Whole cluster steps

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
