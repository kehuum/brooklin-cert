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

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')


class BasicTests(unittest.TestCase):
    """All basic certification tests"""

    def test_basic(self):
        datastream_name = 'test_basic'
        create_datastream = \
            (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
             CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep = Sleep(secs=60 * 15)

        run_ekg = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time, endtime_getter=sleep.end_time)

        kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time, endtime_getter=sleep.end_time,
                                     topics_file='data/voyager-topics.txt'),
                       RunKafkaAudit(starttime_getter=create_datastream[1].end_time, endtime_getter=sleep.end_time,
                                     topics_file='data/experiment-voyager-topics.txt'))

        builder = TestRunnerBuilder('test_basic') \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep) \
            .add_sequential(run_ekg) \
            .add_parallel(*kafka_audit) \
            .build()

        self.assertTrue(builder.run())

    def test_restart_datastream(self):
        datastream_name = 'test-restart-datastream'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_restart = Sleep(secs=60 * 10)

        restart_datastream = (RestartDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name),
                              RestartDatastream(cluster=BrooklinClusterChoice.EXPERIMENT, name=datastream_name))

        sleep_after_restart = Sleep(secs=60 * 10)

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_after_restart.end_time)

        kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                     endtime_getter=sleep_after_restart.end_time,
                                     topics_file='data/voyager-topics.txt'),
                       RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                     endtime_getter=sleep_after_restart.end_time,
                                     topics_file='data/experiment-voyager-topics.txt'))

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_restart) \
            .add_parallel(*restart_datastream) \
            .add_sequential(sleep_after_restart) \
            .add_sequential(ekg_analysis) \
            .add_parallel(*kafka_audit) \
            .build()

        self.assertTrue(builder.run())

    def test_update_datastream_whitelist(self):
        control_topic_prefixes = ['voyager-api', 'seas-']
        experiment_topic_prefixes = ['experiment-voyager-api', 'experiment-seas-']

        list_topics_source = \
            (ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=control_topic_prefixes),
             ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=experiment_topic_prefixes))

        datastream_name = 'test_update_datastream_whitelist'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_update = Sleep(secs=60 * 10)

        update_datastream = (UpdateDatastream(whitelist='^(^voyager-api.*$)|(^seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.CONTROL),
                             UpdateDatastream(whitelist='^(^experiment-voyager-api.*$)|(^experiment-seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.EXPERIMENT))

        sleep_after_update = Sleep(secs=60 * 10)

        list_topics_destination_after_update = \
            (ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefixes_filter=control_topic_prefixes),
             ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefixes_filter=experiment_topic_prefixes))

        validate_topics_after_update = \
            (ValidateSourceAndDestinationTopicsMatch(
                source_topics_getter=list_topics_source[0].get_topics,
                destination_topics_getter=list_topics_destination_after_update[0].get_topics),
             ValidateSourceAndDestinationTopicsMatch(
                 source_topics_getter=list_topics_source[1].get_topics,
                 destination_topics_getter=list_topics_destination_after_update[1].get_topics))

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_after_update.end_time)

        kafka_audit_basic = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                           endtime_getter=sleep_after_update.end_time,
                                           topics_file='data/voyager-topics.txt'),
                             RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                           endtime_getter=sleep_after_update.end_time,
                                           topics_file='data/experiment-voyager-topics.txt'))

        kafka_audit_new_topics = (RunKafkaAudit(starttime_getter=update_datastream[0].end_time,
                                                endtime_getter=sleep_after_update.end_time,
                                                topics_file='data/voyager-seas-topics.txt'),
                                  RunKafkaAudit(starttime_getter=update_datastream[1].end_time,
                                                endtime_getter=sleep_after_update.end_time,
                                                topics_file='data/experiment-voyager-seas-topics.txt'))

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_parallel(*list_topics_source) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_update) \
            .add_parallel(*update_datastream) \
            .add_sequential(sleep_after_update) \
            .add_parallel(*list_topics_destination_after_update) \
            .add_parallel(*validate_topics_after_update) \
            .add_sequential(ekg_analysis) \
            .add_parallel(*kafka_audit_basic) \
            .add_parallel(*kafka_audit_new_topics) \
            .build()

        self.assertTrue(builder.run())

    def test_multiple_topic_creation_with_traffic(self):
        control_topics_list = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]

        def get_control_topics_list():
            return control_topics_list

        experiment_topics_list = [f'experiment-voyager-api-bmm-certification-test-{i}' for i in range(10)]

        def get_experiment_topics_list():
            return control_topics_list

        datastream_name = 'test_multiple_topic_creation_with_traffic'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_topics_creation = Sleep(secs=60 * 5)

        create_control_topics = (CreateSourceTopics(topics=control_topics_list, delay_seconds=60 * 2),
                                 CreateSourceTopics(topics=experiment_topics_list, delay_seconds=60 * 2))

        wait_for_topic_on_destination = (ValidateDestinationTopicsExist(topics_getter=get_control_topics_list),
                                         ValidateDestinationTopicsExist(topics_getter=get_experiment_topics_list))

        produce_traffic = (ProduceToSourceTopics(topics=control_topics_list, num_records=10000, record_size=1000),
                           ProduceToSourceTopics(topics=experiment_topics_list, num_records=10000, record_size=1000))

        sleep_after_producing_traffic = Sleep(secs=60 * 5)

        consume_records = (ConsumeFromDestinationTopics(topics=control_topics_list, num_records=10000),
                           ConsumeFromDestinationTopics(topics=experiment_topics_list, num_records=10000))

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_after_producing_traffic.end_time)

        kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                     endtime_getter=sleep_after_producing_traffic.end_time,
                                     topics_file='data/voyager-topics.txt'),
                       RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                     endtime_getter=sleep_after_producing_traffic.end_time,
                                     topics_file='data/experiment-voyager-topics.txt'))

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_topics_creation) \
            .add_parallel(*create_control_topics)

        if DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*wait_for_topic_on_destination)

        builder.add_parallel(*produce_traffic) \
            .add_sequential(sleep_after_producing_traffic)

        if not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*wait_for_topic_on_destination)

        builder.add_parallel(*consume_records) \
            .add_sequential(ekg_analysis) \
            .add_parallel(*kafka_audit) \
            .build()

        self.assertTrue(builder.run())


class BrooklinClusterBounceTests(unittest.TestCase):
    """All tests involving cluster restarts that use LID"""

    def test_brooklin_cluster_parallel_bounce(self):
        self.assertTrue(restart_brooklin_cluster('test_brooklin_cluster_parallel_bounce', 100)
                        .run())

    def test_brooklin_cluster_rolling_bounce(self):
        self.assertTrue(restart_brooklin_cluster('test_brooklin_cluster_rolling_bounce', 10)
                        .run())


class BrooklinErrorInducingTests(unittest.TestCase):
    """All Brooklin error-inducing certification tests"""

    def test_kill_random_brooklin_host(self):
        self.assertTrue(kill_start_brooklin_host('test_kill_random_brooklin_host', False)
                        .run())

    def test_kill_leader_brooklin_host(self):
        self.assertTrue(kill_start_brooklin_host('test_kill_leader_brooklin_host', True)
                        .run())

    def test_stop_random_brooklin_host(self):
        self.assertTrue(stop_start_brooklin_host('test_stop_random_brooklin_host', False)
                        .run())

    def test_stop_leader_brooklin_host(self):
        self.assertTrue(stop_start_brooklin_host('test_stop_leader_brooklin_host', True)
                        .run())

    @unittest.skip("Postponed until ZK session expiry fixes are made")
    def test_pause_resume_random_brooklin_host(self):
        self.assertTrue(pause_resume_brooklin_host('test_pause_resume_random_brooklin_host', False)
                        .run())

    @unittest.skip("Postponed until ZK session expiry fixes are made")
    def test_pause_resume_leader_brooklin_host(self):
        self.assertTrue(pause_resume_brooklin_host('test_pause_resume_leader_brooklin_host', True)
                        .run())


class KafkaErrorInducingTests(unittest.TestCase):
    """All Kafka error-inducing certification tests"""

    def test_kill_random_source_kafka_broker(self):
        self.assertTrue(kill_kafka_broker('test_kill_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
                        .run())

    def test_kill_random_destination_kafka_broker(self):
        self.assertTrue(kill_kafka_broker('test_kill_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
                        .run())

    def test_stop_random_source_kafka_broker(self):
        self.assertTrue(stop_kafka_broker('test_stop_random_source_kafka_broker', KafkaClusterChoice.SOURCE)
                        .run())

    def test_stop_random_destination_kafka_broker(self):
        self.assertTrue(stop_kafka_broker('test_stop_random_destination_kafka_broker', KafkaClusterChoice.DESTINATION)
                        .run())

    def test_perform_ple_source_kafka_cluster(self):
        self.assertTrue(perform_kafka_ple('test_perform_ple_source_kafka_cluster', KafkaClusterChoice.SOURCE)
                        .run())

    def test_perform_ple_destination_kafka_cluster(self):
        self.assertTrue(perform_kafka_ple('test_perform_ple_destination_kafka_cluster', KafkaClusterChoice.DESTINATION)
                        .run())

    def test_restart_source_kafka_cluster(self):
        self.assertTrue(restart_kafka_cluster('test_restart_source_kafka_cluster', KafkaClusterChoice.SOURCE, 10)
                        .run())

    def test_restart_destination_kafka_cluster(self):
        self.assertTrue(
            restart_kafka_cluster('test_restart_destination_kafka_cluster', KafkaClusterChoice.DESTINATION, 10)
            .run())


if __name__ == '__main__':
    unittest.main()
