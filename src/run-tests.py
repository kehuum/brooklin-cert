#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.teststeps import CreateDatastream, ClusterChoice, RestartDatastream
from testlib.brooklin.testhelpers import kill_brooklin_host, stop_brooklin_host, pause_resume_brooklin_host
from testlib.core.runner import TestRunner
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class BasicTests(unittest.TestCase):
    """All basic certification tests"""

    def test_basic(self):
        datastream_name = 'test_basic'
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, name=datastream_name, topic_create=True,
                                              identity=False, passthrough=False, partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep = Sleep(secs=60 * 15)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)

        # TODO: Add a step for running audit on the experiment data-flow

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        self.assertTrue(TestRunner('test_basic').run(control_datastream, sleep, kafka_audit, run_ekg))

    def test_restart_datastream(self):
        datastream_name = 'test-restart-datastream'
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, name=datastream_name,
                                              topic_create=True, identity=False, passthrough=False,
                                              partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep_before_restart = Sleep(secs=60 * 10)
        restart_datastream = RestartDatastream(cluster=ClusterChoice.CONTROL, name=datastream_name)

        # TODO: Add a step for restarting experiment datastream

        sleep_after_restart = Sleep(secs=60 * 10)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_restart.end_time)

        # TODO: Add a step for running audit on the experiment data-flow

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                 endtime_getter=sleep_after_restart.end_time)
        self.assertTrue(TestRunner('test_restart_datastream').run(control_datastream, sleep_before_restart,
                                                                  restart_datastream, sleep_after_restart, kafka_audit,
                                                                  run_ekg))


class BrooklinErrorInducingTests(unittest.TestCase):
    """All Brooklin error-inducing certification tests"""

    def test_kill_random_brooklin_host(self):
        test_steps = kill_brooklin_host("test_kill_random_brooklin_host", False)
        self.assertTrue(TestRunner('test_kill_random_brooklin_host').run(*test_steps))

    def test_kill_leader_brooklin_host(self):
        test_steps = kill_brooklin_host("test_kill_leader_brooklin_host", True)
        self.assertTrue(TestRunner('test_kill_leader_brooklin_host').run(*test_steps))

    def test_stop_random_brooklin_host(self):
        test_steps = stop_brooklin_host("test_stop_random_brooklin_host", False)
        self.assertTrue(TestRunner('test_stop_random_brooklin_host').run(*test_steps))

    def test_stop_leader_brooklin_host(self):
        test_steps = stop_brooklin_host("test_stop_leader_brooklin_host", True)
        self.assertTrue(TestRunner('test_stop_leader_brooklin_host').run(*test_steps))

    def test_pause_resume_random_brooklin_host(self):
        test_steps = pause_resume_brooklin_host("test_pause_resume_random_brooklin_host", False)
        self.assertTrue(TestRunner('test_pause_resume_random_brooklin_host').run(*test_steps))

    def test_pause_resume_leader_brooklin_host(self):
        test_steps = pause_resume_brooklin_host("test_pause_resume_leader_brooklin_host", True)
        self.assertTrue(TestRunner('test_pause_resume_leader_brooklin_host').run(*test_steps))


if __name__ == '__main__':
    unittest.main()
