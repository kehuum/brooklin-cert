#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.teststeps import CreateDatastream, ClusterChoice, RestartDatastream
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit
from testlib.core.runner import TestRunner
from testlib.core.teststeps import Sleep

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class BasicTests(unittest.TestCase):
    """All basic certification tests"""

    def test_basic(self):
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, topic_create=True, identity=False,
                                              passthrough=False, partition_managed=True)

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


if __name__ == '__main__':
    unittest.main()
