import json

from collections import namedtuple
from enum import Enum
from testlib.brooklin.environment import BrooklinClusterChoice


class Datastream(object):
    """Represents a Brooklin datastream

    Exposes utility methods for Brooklin datastreams which are represented
    as plain Python dictionaries obtained from deserializing datastreams
    JSON representation

    """

    def __init__(self, datastream: dict):
        self.datastream = datastream

    def __str__(self):
        return json.dumps(self.datastream, indent=4)

    @property
    def status(self):
        return self.datastream.get('Status')

    @property
    def is_ready(self):
        return self.status == 'READY'

    @property
    def is_paused(self):
        return self.status == 'PAUSED'

    @property
    def is_stopped(self):
        return self.status == 'STOPPED'

    @property
    def whitelist(self):
        conn_str = self.datastream.get("source", {}).get("connectionString", '')
        return conn_str[conn_str.rfind('/') + 1:]


DatastreamCreationInfo = namedtuple('DatastreamCreationInfo', ['cluster', 'num_tasks', 'topic_create', 'identity',
                                                               'passthrough', 'partition_managed'])


class DatastreamConfigChoice(Enum):
    CONTROL = DatastreamCreationInfo(cluster=BrooklinClusterChoice.CONTROL,
                                     num_tasks=120,
                                     topic_create=True,
                                     identity=False,
                                     passthrough=False,
                                     partition_managed=False)

    EXPERIMENT = DatastreamCreationInfo(cluster=BrooklinClusterChoice.EXPERIMENT,
                                        num_tasks=120,
                                        topic_create=True,
                                        identity=False,
                                        passthrough=False,
                                        partition_managed=True)