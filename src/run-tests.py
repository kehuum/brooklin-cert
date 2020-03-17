#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.teststeps import CreateDatastream, ClusterChoice, RestartDatastream, KillBrooklinHost, \
    StartBrooklinHost, StopBrooklinHost, GetBrooklinLeaderHost, KillRandomBrooklinHost, StopRandomBrooklinHost
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit
from testlib.core.runner import TestRunner
from testlib.core.teststeps import Sleep, TestStep

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
        test_steps = BrooklinErrorInducingTests.kill_brooklin_host("test_kill_random_brooklin_host", False)
        self.assertTrue(TestRunner('test_kill_random_brooklin_host').run(*test_steps))

    def test_kill_leader_brooklin_host(self):
        test_steps = BrooklinErrorInducingTests.kill_brooklin_host("test_kill_leader_brooklin_host", True)
        self.assertTrue(TestRunner('test_kill_leader_brooklin_host').run(*test_steps))

    def test_stop_random_brooklin_host(self):
        test_steps = BrooklinErrorInducingTests.stop_brooklin_host("test_stop_random_brooklin_host", False)
        self.assertTrue(TestRunner('test_stop_random_brooklin_host').run(*test_steps))

    def test_stop_leader_brooklin_host(self):
        test_steps = BrooklinErrorInducingTests.stop_brooklin_host("test_stop_leader_brooklin_host", True)
        self.assertTrue(TestRunner('test_stop_leader_brooklin_host').run(*test_steps))

    @staticmethod
    def kill_brooklin_host(datastream_name, is_leader):
        test_steps = []
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, name=datastream_name, topic_create=True,
                                              identity=False, passthrough=False, partition_managed=True)
        test_steps.append(control_datastream)

        # TODO: Add a step for creating experiment datastream

        sleep_before_kill = Sleep(secs=60 * 10)
        test_steps.append(sleep_before_kill)

        if is_leader:
            find_leader_host = GetBrooklinLeaderHost(cluster=ClusterChoice.CONTROL)
            test_steps.append(find_leader_host)

            # TODO: Add a step for finding the leader Brooklin host in the experiment cluster

            kill_brooklin_host = KillBrooklinHost(hostname_getter=find_leader_host.get_leader_host)
            test_steps.append(kill_brooklin_host)

            # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

            host_getter = find_leader_host.get_leader_host
        else:
            kill_brooklin_host = KillRandomBrooklinHost(cluster=ClusterChoice.CONTROL)
            test_steps.append(kill_brooklin_host)

            # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

            host_getter = kill_brooklin_host.get_host

        test_steps.append(Sleep(secs=60))
        test_steps.append(StartBrooklinHost(host_getter))

        # TODO: Add a step for starting the killed Brooklin host in the experiment cluster

        sleep_after_start = Sleep(secs=60 * 10)
        test_steps.append(sleep_after_start)
        test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                        endtime_getter=sleep_after_start.end_time))

        # TODO: Add a step for running audit on the experiment data-flow

        test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                         endtime_getter=sleep_after_start.end_time))
        return test_steps

    @staticmethod
    def stop_brooklin_host(datastream_name, is_leader):
        test_steps = []
        control_datastream = CreateDatastream(cluster=ClusterChoice.CONTROL, name=datastream_name, topic_create=True,
                                              identity=False, passthrough=False, partition_managed=True)
        test_steps.append(control_datastream)

        # TODO: Add a step for creating experiment datastream

        sleep_before_stop = Sleep(secs=60 * 10)
        test_steps.append(sleep_before_stop)

        if is_leader:
            find_leader_host = GetBrooklinLeaderHost(cluster=ClusterChoice.CONTROL)
            test_steps.append(find_leader_host)

            # TODO: Add a step for finding the leader Brooklin host in the experiment cluster

            stop_brooklin_host = StopBrooklinHost(hostname_getter=find_leader_host.get_leader_host)
            test_steps.append(stop_brooklin_host)

            # TODO: Add a step for stopping the leader Brooklin host in the experiment cluster

            host_getter = find_leader_host.get_leader_host
        else:
            stop_brooklin_host = StopRandomBrooklinHost(cluster=ClusterChoice.CONTROL)
            test_steps.append(stop_brooklin_host)

            # TODO: Add a step for stopping the Brooklin host in the experiment cluster

            host_getter = stop_brooklin_host.get_host

        test_steps.append(Sleep(secs=60))
        test_steps.append(StartBrooklinHost(host_getter))

        # TODO: Add a step for starting the Brooklin host in the experiment cluster

        sleep_after_start = Sleep(secs=60 * 10)
        test_steps.append(sleep_after_start)
        test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                        endtime_getter=sleep_after_start.end_time))

        # TODO: Add a step for running audit on the experiment data-flow

        test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                         endtime_getter=sleep_after_start.end_time))
        return test_steps


if __name__ == '__main__':
    unittest.main()
