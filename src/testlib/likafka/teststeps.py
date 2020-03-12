from testlib.core.teststeps import RunPythonCommand


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit"""

    def __init__(self, starttime_getter, endtime_getter, topics_file='data/topics.txt'):
        super().__init__()
        if not topics_file:
            raise ValueError(f'Invalid topics file: {topics_file}')

        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.topics_file = topics_file
        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter

    @property
    def main_command(self):
        return 'kafka-audit-v2.py ' \
               f'--topicsfile {self.topics_file} ' \
               f'--startms {self.starttime_getter()} ' \
               f'--endms {self.endtime_getter()}'
