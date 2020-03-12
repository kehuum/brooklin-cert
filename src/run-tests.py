#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.teststeps import CreateDatastream, ClusterChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit
from testlib.core.runner import TestRunner
from testlib.core.teststeps import Sleep

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class LiveMirroringTests(unittest.TestCase):
    """All tests involving mirroring live data"""

    def test_basic_mirroring(self):
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, topic_create=True, identity=False,
                                              passthrough=False, partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep = Sleep(secs=60 * 15)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        self.assertTrue(TestRunner('test_basic_mirroring').run(control_datastream, sleep, kafka_audit, run_ekg))


if __name__ == '__main__':
    unittest.main()
