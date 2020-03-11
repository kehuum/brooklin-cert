#!/usr/bin/env python3

import logging
import subprocess
import time
import unittest

from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum
from typing import Callable
from common import typename

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class TestStep(ABC):
    """Base class of all test steps

    It provides two methods, start_time() and end_time(), that capture the test
    step's start and end of execution times in seconds since the epoch.

    Extenders are expected to:
        - Implement run_test() to specify what the test step should do
        - Optionally override cleanup() to add logic to clean up any resources
          created during the test to reset state before the next test
    """

    def __init__(self):
        self.__start_time = None
        self.__end_time = None

    def start_time(self):
        return int(self.__start_time) if self.__start_time else None

    def end_time(self):
        return int(self.__end_time) if self.__end_time else None

    def run(self):
        self.__start_time = time.time()
        self.run_test()
        self.__end_time = time.time()

    def cleanup(self):
        pass

    @abstractmethod
    def run_test(self):
        pass


class Sleep(TestStep):
    """Test step to sleep"""

    def __init__(self, secs):
        super().__init__()
        if secs <= 0:
            raise ValueError('secs must be greater than zero')
        self._secs = secs

    def run_test(self):
        logging.info(f'Sleeping for {self._secs} seconds')
        time.sleep(self._secs)


class RunPythonCommand(TestStep, ABC):
    """Base class of any test step that invovles running a Python script.

    Extenders are expected to:
        - Implement the main_command property to specify the Python script to run
        - Optionally override the cleanup_command property to specify a Python
          script to run whether or not executing main_command failed or not.
    """
    PYTHON_PATH = '/export/apps/python/3.7/bin/python3'

    def run_test(self):
        command = self.main_command
        logging.info(f'Running Python command: {command}')
        RunPythonCommand.run_command(command)

    def cleanup(self):
        command = self.cleanup_command
        if command:
            logging.info(f'Running Python cleanup command: {command}')
            RunPythonCommand.run_command(self.cleanup_command)

    @property
    @abstractmethod
    def main_command(self):
        pass

    @property
    def cleanup_command(self):
        return ''

    @staticmethod
    def run_command(command, timeout=60 * 60 * 15):
        subprocess.run(f'{RunPythonCommand.PYTHON_PATH} {command}', check=True, shell=True, timeout=timeout,
                       universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit"""

    def __init__(self, starttime_getter, endtime_getter, topics_file='../data/topics.txt'):
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


class RunEkgAnalysis(RunPythonCommand):
    """Test step for running EKG analysis
    """

    def __init__(self, starttime_getter, endtime_getter):
        super().__init__()
        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter

    @property
    def main_command(self):
        return 'ekg-client.py ' \
               '--cf prod-lor1 --ct cert.control ' \
               '--ef prod-lor1 --et cert.candidate ' \
               f'-s {self.starttime_getter()} ' \
               f'-e {self.endtime_getter()}'


class TestRunner(object):
    """Main test driver responsible for running an end-to-end test"""

    def __init__(self, test_name):
        self.test_name = test_name

    def run(self, *steps: TestStep):
        test_success = True
        cleanup_steps = []

        for s in steps:
            step_name = f'{self.test_name}:{typename(s)}'
            cleanup_steps.append(s)

            logging.info(f'Running test step {step_name}')
            success, message = self.execute(s)
            if not success:
                logging.error(f'Test step {step_name} failed with an error: {message}')
                test_success = False
                break

        for c in reversed(cleanup_steps):
            step_name = f'{self.test_name}:{typename(c)}'
            logging.info(f'Running cleanup test step {step_name}')
            success, message = self.cleanup(c)
            if not success:
                logging.error(f'Cleanup test step {step_name} failed with an error: {message}')
                test_success = False

        return test_success

    @staticmethod
    def execute(step: TestStep):
        return TestRunner.execute_fn(fn=step.run, step_name=typename(step))

    @staticmethod
    def cleanup(step: TestStep):
        return TestRunner.execute_fn(fn=step.cleanup, step_name=typename(step))

    @staticmethod
    def execute_fn(fn: Callable[[], None], step_name) -> (bool, str):
        try:
            fn()
        except Exception as err:
            error_message = [f'Test step {step_name} failed with error: {err}']
            if isinstance(err, subprocess.CalledProcessError):
                if err.stdout:
                    error_message += [f'stdout:\n{err.stdout.strip()}']
                if err.stderr:
                    error_message += [f'stderr:\n{err.stderr.strip()}']
            return False, '\n'.join(error_message)
        else:
            return True, ''


class LiveMirroringTests(unittest.TestCase):
    """All tests involving mirroring live data"""

    def test_basic_mirroring(self):
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, topic_create=True, identity=False,
                                              passthrough=False, partition_managed=True)

        # TODO: Add a step for creating experiement datastream

        sleep = Sleep(secs=60 * 15)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        self.assertTrue(TestRunner('test_basic_mirroring').run(control_datastream, sleep, kafka_audit, run_ekg))


if __name__ == '__main__':
    unittest.main()
