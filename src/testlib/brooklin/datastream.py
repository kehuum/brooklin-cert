import json


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

    def is_ready(self):
        return self.status == 'READY'

    def is_paused(self):
        return self.status == 'PAUSED'

    def is_stopped(self):
        return self.status == 'STOPPED'

    @property
    def whitelist(self):
        conn_str = self.datastream.get("source", {}).get("connectionString", '')
        return conn_str[conn_str.rfind('/') + 1:]
