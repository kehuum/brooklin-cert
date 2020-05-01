#!/usr/bin/env python3

import logging
import unittest

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.testhelpers import kill_start_brooklin_host, stop_start_brooklin_host, \
    pause_resume_brooklin_host, restart_brooklin_cluster
from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, RestartDatastream, UpdateDatastream
from testlib.core.runner import TestRunnerBuilder
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.testhelpers import kill_kafka_broker, stop_kafka_broker, perform_kafka_ple, \
    restart_kafka_cluster
from testlib.likafka.teststeps import RunKafkaAudit, KafkaClusterChoice, ListTopics, \
    ValidateSourceAndDestinationTopicsMatch, CreateSourceTopics, ValidateDestinationTopicsExist, \
    ProduceToSourceTopics, ConsumeFromDestinationTopics

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class BasicTests(unittest.TestCase):
    """All basic certification tests"""

    def test_basic(self):
        datastream_name = 'test_basic'
        control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)

        # TODO: Add a step for creating experiment datastream

        sleep = Sleep(secs=60 * 15)
        kafka_audit = RunKafkaAudit(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)

        # TODO: Add a step for running audit on the experiment data-flow

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time, endtime_getter=sleep.end_time)
        self.assertTrue(TestRunnerBuilder('test_basic')
                        .add_sequential(control_datastream, sleep, kafka_audit, run_ekg)
                        .build().run())

    def test_restart_datastream(self):
        datastream_name = 'test-restart-datastream'
        control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)

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
        self.assertTrue(TestRunnerBuilder('test_restart_datastream')
                        .add_sequential(control_datastream, sleep_before_restart, restart_datastream,
                                        sleep_after_restart, kafka_audit, run_ekg)
                        .build().run())

    def test_update_datastream_whitelist(self):
        topic_prefixes = ['voyager-api', 'seas-']
        list_topics_source = ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=topic_prefixes)

        datastream_name = 'test_update_datastream_whitelist'
        control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)

        # TODO: Add a step for creating experiment datastream

        sleep_before_update = Sleep(secs=60 * 10)
        update_datastream = UpdateDatastream(whitelist='^(^voyager-api.*$)|(^seas-.*$)',
                                             metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                             cluster=BrooklinClusterChoice.CONTROL)

        # TODO: Add a step for updating experiment datastream

        sleep_after_update = Sleep(secs=60 * 10)

        list_topics_destination_after_update = \
            ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefixes_filter=topic_prefixes)
        validate_topics_after_update = \
            ValidateSourceAndDestinationTopicsMatch(
                source_topics_getter=list_topics_source.get_topics,
                destination_topics_getter=list_topics_destination_after_update.get_topics)

        # TODO: Add a step for validating topics for experiment datastream's whitelist

        # This audit step validates the first whitelist's completeness since the datastream was initially created
        kafka_audit_basic = RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                          endtime_getter=sleep_after_update.end_time)

        # TODO: Add a step for running audit on the experiment data-flow for the older whitelist

        # This audit step validates the second whitelist's completeness, and it is only fair to compare counts for the
        # newer topics from when the whitelist was updated
        kafka_audit_new_topics = RunKafkaAudit(starttime_getter=update_datastream.end_time,
                                               endtime_getter=sleep_after_update.end_time,
                                               topics_file='data/voyager-seas-topics.txt')

        # TODO: Add a step for running audit on the experiment data-flow for the newer whitelist

        run_ekg = RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                 endtime_getter=sleep_after_update.end_time)

        self.assertTrue(TestRunnerBuilder('test_update_datastream_whitelist')
                        .add_sequential(list_topics_source, control_datastream, sleep_before_update, update_datastream,
                                        sleep_after_update, list_topics_destination_after_update,
                                        validate_topics_after_update, kafka_audit_basic, kafka_audit_new_topics,
                                        run_ekg)
                        .build().run())

    def test_multiple_topic_creation_with_traffic(self):
        test_steps = []
        control_topics_list = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]

        def get_control_topics_list():
            return control_topics_list

        # TODO: Create list of topics matching experiment datastream whitelist

        datastream_name = 'test_multiple_topic_creation_with_traffic'
        control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)
        test_steps.append(control_datastream)

        # TODO: Add a step for creating experiment datastream

        sleep_before_topics_creation = Sleep(secs=60 * 5)
        test_steps.append(sleep_before_topics_creation)

        create_control_topics = CreateSourceTopics(topics=control_topics_list, delay_seconds=60 * 2)
        test_steps.append(create_control_topics)

        # TODO: Add test steps for creating topics matching the experiment datasteam whitelist

        # The ordering of when topic existence is validated depends on whether auto topic creation is enabled or not
        wait_for_topic_on_destination = ValidateDestinationTopicsExist(topics_getter=get_control_topics_list)
        if DatastreamConfigChoice.CONTROL.value.topic_create:
            test_steps.append(wait_for_topic_on_destination)

        # TODO: Add test steps to wait for topics on destination for the experiment datasteam whitelist

        produce_traffic = ProduceToSourceTopics(topics=control_topics_list, num_records=10000, record_size=1000)
        test_steps.append(produce_traffic)

        # TODO: Add test steps for producing traffic to the experiment datasteam's new topics

        sleep_after_producing_traffic = Sleep(secs=60 * 5)
        test_steps.append(sleep_after_producing_traffic)

        if not DatastreamConfigChoice.CONTROL.value.topic_create:
            test_steps.append(wait_for_topic_on_destination)

        consume_records = ConsumeFromDestinationTopics(topics=control_topics_list, num_records=10000)
        test_steps.append(consume_records)

        # TODO: Add test steps for consuming records from the experiment datasteam's new topics

        test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                        endtime_getter=sleep_after_producing_traffic.end_time))

        # TODO: Add a step for running audit on the experiment data-flow

        test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                         endtime_getter=sleep_after_producing_traffic.end_time))

        self.assertTrue(TestRunnerBuilder('test_multiple_topic_creation_with_traffic')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_brooklin_cluster_parallel_bounce(self):
        test_steps = restart_brooklin_cluster('test_brooklin_cluster_parallel_bounce', 100)
        self.assertTrue(TestRunnerBuilder('test_brooklin_cluster_parallel_bounce')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_brooklin_cluster_rolling_bounce(self):
        test_steps = restart_brooklin_cluster('test_brooklin_cluster_rolling_bounce', 10)
        self.assertTrue(TestRunnerBuilder('test_brooklin_cluster_rolling_bounce')
                        .add_sequential(*test_steps)
                        .build().run())


class BrooklinErrorInducingTests(unittest.TestCase):
    """All Brooklin error-inducing certification tests"""

    def test_kill_random_brooklin_host(self):
        test_steps = kill_start_brooklin_host('test_kill_random_brooklin_host', False)
        self.assertTrue(TestRunnerBuilder('test_kill_random_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_kill_leader_brooklin_host(self):
        test_steps = kill_start_brooklin_host('test_kill_leader_brooklin_host', True)
        self.assertTrue(TestRunnerBuilder('test_kill_leader_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_stop_random_brooklin_host(self):
        test_steps = stop_start_brooklin_host('test_stop_random_brooklin_host', False)
        self.assertTrue(TestRunnerBuilder('test_stop_random_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_stop_leader_brooklin_host(self):
        test_steps = stop_start_brooklin_host('test_stop_leader_brooklin_host', True)
        self.assertTrue(TestRunnerBuilder('test_stop_leader_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_pause_resume_random_brooklin_host(self):
        test_steps = pause_resume_brooklin_host('test_pause_resume_random_brooklin_host', False)
        self.assertTrue(TestRunnerBuilder('test_pause_resume_random_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_pause_resume_leader_brooklin_host(self):
        test_steps = pause_resume_brooklin_host('test_pause_resume_leader_brooklin_host', True)
        self.assertTrue(TestRunnerBuilder('test_pause_resume_leader_brooklin_host')
                        .add_sequential(*test_steps)
                        .build().run())


class KafkaErrorInducingTests(unittest.TestCase):
    """All Kafka error-inducing certification tests"""

    def test_kill_random_source_kafka_broker(self):
        test_steps = kill_kafka_broker('test_kill_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
        self.assertTrue(TestRunnerBuilder('test_kill_random_source_kafka_broker')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_kill_random_destination_kafka_broker(self):
        test_steps = kill_kafka_broker('test_kill_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
        self.assertTrue(TestRunnerBuilder('test_kill_random_destination_kafka_broker')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_stop_random_source_kafka_broker(self):
        test_steps = stop_kafka_broker('test_stop_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
        self.assertTrue(TestRunnerBuilder('test_stop_random_source_kafka_broker')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_stop_random_destination_kafka_broker(self):
        test_steps = stop_kafka_broker('test_stop_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
        self.assertTrue(TestRunnerBuilder('test_stop_random_destination_kafka_broker')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_perform_ple_source_kafka_cluster(self):
        test_steps = perform_kafka_ple('test_perform_ple_source_kafka_cluster', KafkaClusterChoice.SOURCE)
        self.assertTrue(TestRunnerBuilder('test_perform_ple_source_kafka_cluster')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_perform_ple_destination_kafka_cluster(self):
        test_steps = perform_kafka_ple('test_perform_ple_destination_kafka_cluster', KafkaClusterChoice.DESTINATION)
        self.assertTrue(TestRunnerBuilder('test_perform_ple_destination_kafka_cluster')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_restart_source_kafka_cluster(self):
        test_steps = restart_kafka_cluster('test_restart_source_kafka_cluster', KafkaClusterChoice.SOURCE, 10)
        self.assertTrue(TestRunnerBuilder('test_restart_source_kafka_cluster')
                        .add_sequential(*test_steps)
                        .build().run())

    def test_restart_destination_kafka_cluster(self):
        test_steps = restart_kafka_cluster('test_restart_destination_kafka_cluster', KafkaClusterChoice.DESTINATION, 10)
        self.assertTrue(TestRunnerBuilder('test_restart_destination_kafka_cluster')
                        .add_sequential(*test_steps)
                        .build().run())


if __name__ == '__main__':
    unittest.main()
