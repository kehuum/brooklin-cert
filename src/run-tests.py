#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, RestartDatastream, UpdateDatastream
from testlib.brooklin.testhelpers import kill_brooklin_host, stop_brooklin_host, pause_resume_brooklin_host
from testlib.core.runner import TestRunner
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.testhelpers import kill_kafka_broker, stop_kafka_broker
from testlib.likafka.teststeps import RunKafkaAudit, KafkaClusterChoice, DeleteTopics, ListTopics, \
    ValidateTopicsDoNotExist, ValidateSourceAndDestinationTopicsMatch

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class BasicTests(unittest.TestCase):
    """All basic certification tests"""

    def test_basic(self):
        datastream_name = 'test_basic'
        control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                              topic_create=True, identity=False, passthrough=False,
                                              partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep = Sleep(secs=60 * 15)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)

        # TODO: Add a step for running audit on the experiment data-flow

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        self.assertTrue(TestRunner('test_basic').run(control_datastream, sleep, kafka_audit, run_ekg))

    def test_restart_datastream(self):
        datastream_name = 'test-restart-datastream'
        control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                              topic_create=True, identity=False, passthrough=False,
                                              partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep_before_restart = Sleep(secs=60 * 10)
        restart_datastream = RestartDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name)

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

    def test_update_datastream_whitelist(self):
        list_topics_source_voyager = ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefix_filter='voyager-api')
        list_topics_source_seas = ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefix_filter='seas-')

        datastream_name = 'test_update_datastream_whitelist'
        control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                              topic_create=True, identity=False, passthrough=False,
                                              partition_managed=True)

        # TODO: Add a step for creating experiment datastream

        sleep_before_update = Sleep(secs=60 * 10)
        update_datastream = UpdateDatastream(whitelist='^(^voyager-api.*$)|(^seas-.*$)',
                                             metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                             cluster=BrooklinClusterChoice.CONTROL)

        # TODO: Add a step for updating experiment datastream

        sleep_after_update = Sleep(secs=60 * 10)

        list_topics_destination_voyager_after_update = \
            ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefix_filter='voyager-api')
        list_topics_destination_seas_after_update = \
            ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefix_filter='seas-')
        validate_voyager_topics_after_update = \
            ValidateSourceAndDestinationTopicsMatch(
                source_topics_getter=list_topics_source_voyager.get_topics,
                destination_topics_getter=list_topics_destination_voyager_after_update.get_topics)
        validate_seas_topics_after_update = \
            ValidateSourceAndDestinationTopicsMatch(
                source_topics_getter=list_topics_source_seas.get_topics,
                destination_topics_getter=list_topics_destination_seas_after_update.get_topics)

        # TODO: Add a step for validating topics for experiment datastream's whitelist

        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_update.end_time,
                                    topics_file='data/voyager-seas-topics.txt')

        # TODO: Add a step for running audit on the experiment data-flow

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                 endtime_getter=sleep_after_update.end_time)

        # Clean-up the new topics created as part of the new whitelist
        cleanup_seas_topics = \
            DeleteTopics(topics_getter=list_topics_destination_seas_after_update.get_topics,
                         cluster=KafkaClusterChoice.DESTINATION)
        validate_seas_topics_cleaned_up = \
            ValidateTopicsDoNotExist(topics_getter=list_topics_destination_seas_after_update.get_topics,
                                     cluster=KafkaClusterChoice.DESTINATION)

        # TODO: Add a step to remove and validate topic prefix of experiment datastream's second whitelist

        self.assertTrue(TestRunner('test_update_datastream_whitelist')
                        .run(list_topics_source_voyager, list_topics_source_seas, control_datastream,
                             sleep_before_update, update_datastream, sleep_after_update,
                             list_topics_destination_voyager_after_update, list_topics_destination_seas_after_update,
                             validate_voyager_topics_after_update, validate_seas_topics_after_update, kafka_audit,
                             run_ekg, cleanup_seas_topics, validate_seas_topics_cleaned_up))


class BrooklinErrorInducingTests(unittest.TestCase):
    """All Brooklin error-inducing certification tests"""

    def test_kill_random_brooklin_host(self):
        test_steps = kill_brooklin_host('test_kill_random_brooklin_host', False)
        self.assertTrue(TestRunner('test_kill_random_brooklin_host').run(*test_steps))

    def test_kill_leader_brooklin_host(self):
        test_steps = kill_brooklin_host('test_kill_leader_brooklin_host', True)
        self.assertTrue(TestRunner('test_kill_leader_brooklin_host').run(*test_steps))

    def test_stop_random_brooklin_host(self):
        test_steps = stop_brooklin_host('test_stop_random_brooklin_host', False)
        self.assertTrue(TestRunner('test_stop_random_brooklin_host').run(*test_steps))

    def test_stop_leader_brooklin_host(self):
        test_steps = stop_brooklin_host('test_stop_leader_brooklin_host', True)
        self.assertTrue(TestRunner('test_stop_leader_brooklin_host').run(*test_steps))

    def test_pause_resume_random_brooklin_host(self):
        test_steps = pause_resume_brooklin_host('test_pause_resume_random_brooklin_host', False)
        self.assertTrue(TestRunner('test_pause_resume_random_brooklin_host').run(*test_steps))

    def test_pause_resume_leader_brooklin_host(self):
        test_steps = pause_resume_brooklin_host('test_pause_resume_leader_brooklin_host', True)
        self.assertTrue(TestRunner('test_pause_resume_leader_brooklin_host').run(*test_steps))


class KafkaErrorInducingTests(unittest.TestCase):
    """All Kafka error-inducing certification tests"""

    def test_kill_random_source_kafka_broker(self):
        test_steps = kill_kafka_broker('test_kill_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
        self.assertTrue(TestRunner('test_kill_random_source_kafka_broker').run(*test_steps))

    def test_kill_random_destination_kafka_broker(self):
        test_steps = kill_kafka_broker('test_kill_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
        self.assertTrue(TestRunner('test_kill_random_destination_kafka_broker').run(*test_steps))

    def test_stop_random_source_kafka_broker(self):
        test_steps = stop_kafka_broker('test_stop_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
        self.assertTrue(TestRunner('test_stop_random_source_kafka_broker').run(*test_steps))

    def test_stop_random_destination_kafka_broker(self):
        test_steps = stop_kafka_broker('test_stop_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
        self.assertTrue(TestRunner('test_stop_random_destination_kafka_broker').run(*test_steps))


if __name__ == '__main__':
    unittest.main()
