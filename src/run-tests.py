#!/usr/bin/env python3
import argparse
import logging
import sys
import unittest

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.testhelpers import kill_start_brooklin_host, stop_start_brooklin_host, \
    pause_resume_brooklin_host, restart_brooklin_cluster
from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, RestartDatastream, UpdateDatastream
from testlib.core.loader import KafkaAuditTestCaseBase, CustomTestLoader
from testlib.core.runner import TestRunnerBuilder
from testlib.core.teststeps import Sleep, SleepUntilNthMinute
from testlib.data import KafkaTopicFileChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.audit import AddDeferredKafkaAuditInquiry
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.testhelpers import kill_kafka_broker, stop_kafka_broker, perform_kafka_ple, \
    restart_kafka_cluster
from testlib.likafka.teststeps import ListTopics, ValidateSourceAndDestinationTopicsMatch, CreateTopics, \
    ValidateDestinationTopicsExist, ProduceToSourceTopics, ConsumeFromDestinationTopics, CreateSourceTopicsOnDestination

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')
logging.getLogger('kafka').setLevel(logging.WARN)
logging.getLogger('kazoo').setLevel(logging.WARN)


class BasicTests(KafkaAuditTestCaseBase):
    """All basic certification tests"""

    def test_basic(self):
        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test_basic'
        create_datastream = \
            (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
             CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep = Sleep(secs=60 * 20)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        run_ekg = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                 endtime_getter=sleep_until_test_end.end_time)

        kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[0].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                       AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[1].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

        runner = TestRunnerBuilder('test_basic') \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep, sleep_until_test_end, run_ekg, *kafka_audit) \
            .build()

        self.doRunTest(runner)

    def test_restart_datastream(self):
        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test-restart-datastream'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_restart = Sleep(secs=60 * 10)

        restart_datastream = (RestartDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name),
                              RestartDatastream(cluster=BrooklinClusterChoice.EXPERIMENT, name=datastream_name))

        sleep_after_restart = Sleep(secs=60 * 10)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_until_test_end.end_time)

        kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[0].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                       AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[1].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

        runner = TestRunnerBuilder(test_name=datastream_name) \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_restart) \
            .add_parallel(*restart_datastream) \
            .add_sequential(sleep_after_restart, sleep_until_test_end, ekg_analysis, *kafka_audit) \
            .build()

        self.doRunTest(runner)

    def test_update_max_tasks(self):
        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test_update_max_tasks'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_update = Sleep(secs=60 * 10)

        metadata_max_tasks_control = f'maxTasks:{DatastreamConfigChoice.CONTROL.value.num_tasks + 10}'
        metadata_max_tasks_candidate = f'maxTasks:{DatastreamConfigChoice.EXPERIMENT.value.num_tasks + 10}'

        update_datastream = (UpdateDatastream(whitelist=None,
                                              metadata=[metadata_max_tasks_control], name=datastream_name,
                                              cluster=BrooklinClusterChoice.CONTROL),
                             UpdateDatastream(whitelist=None,
                                              metadata=[metadata_max_tasks_candidate], name=datastream_name,
                                              cluster=BrooklinClusterChoice.EXPERIMENT))

        sleep_after_update = Sleep(secs=60 * 10)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_until_test_end.end_time)

        kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[0].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                       AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[1].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

        runner = TestRunnerBuilder(test_name=datastream_name) \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_update) \
            .add_parallel(*update_datastream) \
            .add_sequential(sleep_after_update, sleep_until_test_end, ekg_analysis, *kafka_audit) \
            .build()

        self.doRunTest(runner)

    def test_update_datastream_whitelist(self):
        # The final whitelist in this test will consist of voyager topics and seas topics. The whitelist update will
        # add the seas topics. On the other hand, the voyager topics should already exist on the destination Kafka
        # cluster, whereas the seas topics are to be added as part of the updated whitelist and these should be created
        # on the destination as part of the test. Thus we only need to validate that the seas topics are indeed created
        # on the destination side.
        control_topic_prefixes = ['seas-']
        experiment_topic_prefixes = ['experiment-seas-']

        list_topics_source = \
            (ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=control_topic_prefixes),
             ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=experiment_topic_prefixes))

        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test_update_datastream_whitelist'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        create_destination_topics = (CreateSourceTopicsOnDestination(topics_getter=list_topics_source[0].get_topics),
                                     CreateSourceTopicsOnDestination(topics_getter=list_topics_source[1].get_topics))

        sleep_before_update = Sleep(secs=60 * 10)

        update_datastream = (UpdateDatastream(whitelist='^(^voyager-api.*$)|(^seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.CONTROL),
                             UpdateDatastream(whitelist='^(^experiment-voyager-api.*$)|(^experiment-seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.EXPERIMENT))

        sleep_after_update = Sleep(secs=60 * 20)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        list_topics_destination_after_update = \
            (ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefixes_filter=control_topic_prefixes),
             ListTopics(cluster=KafkaClusterChoice.DESTINATION, topic_prefixes_filter=experiment_topic_prefixes))

        validate_topics_after_update = \
            (ValidateSourceAndDestinationTopicsMatch(
                source_topics_getter=list_topics_source[0].get_topics,
                destination_topics_getter=list_topics_destination_after_update[0].get_topics,
                include_all_topics=create_datastream[0].topic_create),
             ValidateSourceAndDestinationTopicsMatch(
                 source_topics_getter=list_topics_source[1].get_topics,
                 destination_topics_getter=list_topics_destination_after_update[1].get_topics,
                 include_all_topics=create_datastream[1].topic_create))

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_until_test_end.end_time)

        kafka_audit_basic = (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                          starttime_getter=create_datastream[0].end_time,
                                                          endtime_getter=sleep_until_test_end.end_time,
                                                          topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                             AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                          starttime_getter=create_datastream[1].end_time,
                                                          endtime_getter=sleep_until_test_end.end_time,
                                                          topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

        kafka_audit_new_topics = \
            (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                          starttime_getter=update_datastream[0].end_time,
                                          endtime_getter=sleep_until_test_end.end_time,
                                          topics_file_choice=KafkaTopicFileChoice.VOYAGER_SEAS),
             AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                          starttime_getter=update_datastream[1].end_time,
                                          endtime_getter=sleep_until_test_end.end_time,
                                          topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER_SEAS))

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_parallel(*list_topics_source) \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream)

        # If passthrough is enabled but auto-topic creation isn't, we need to manually create topics on the destination
        # Kafka cluster. This mimics what we do in actual PROD clusters with this configuration.
        if DatastreamConfigChoice.CONTROL.value.passthrough and not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*create_destination_topics)

        runner = builder.add_sequential(sleep_before_update) \
            .add_parallel(*update_datastream) \
            .add_sequential(sleep_after_update, sleep_until_test_end) \
            .add_parallel(*list_topics_destination_after_update) \
            .add_parallel(*validate_topics_after_update) \
            .add_sequential(ekg_analysis, *kafka_audit_basic, *kafka_audit_new_topics) \
            .build()

        self.doRunTest(runner)

    def test_multiple_topic_creation_with_traffic(self):
        control_topics = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]
        experiment_topics = [f'experiment-voyager-api-bmm-certification-test-{i}' for i in range(10)]

        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test_multiple_topic_creation_with_traffic'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT))

        sleep_before_topics_creation = Sleep(secs=60 * 5)

        update_datastream_before_create = (UpdateDatastream(whitelist=None,
                                                            metadata=['system.auto.offset.reset:earliest'],
                                                            name=datastream_name,
                                                            cluster=BrooklinClusterChoice.CONTROL),
                                           UpdateDatastream(whitelist=None,
                                                            metadata=['system.auto.offset.reset:earliest'],
                                                            name=datastream_name,
                                                            cluster=BrooklinClusterChoice.EXPERIMENT))

        create_source_topics = (CreateTopics(topics=control_topics, cluster=KafkaClusterChoice.SOURCE,
                                             delay_seconds=60 * 2),
                                CreateTopics(topics=experiment_topics, cluster=KafkaClusterChoice.SOURCE,
                                             delay_seconds=60 * 2))

        create_destination_topics = (CreateTopics(topics=control_topics, cluster=KafkaClusterChoice.DESTINATION,
                                                  delay_seconds=0),
                                     CreateTopics(topics=experiment_topics, cluster=KafkaClusterChoice.DESTINATION,
                                                  delay_seconds=0))

        wait_for_topic_on_destination = (ValidateDestinationTopicsExist(topics=control_topics),
                                         ValidateDestinationTopicsExist(topics=experiment_topics))

        produce_traffic = (ProduceToSourceTopics(topics=control_topics, num_records=10000, record_size=1000),
                           ProduceToSourceTopics(topics=experiment_topics, num_records=10000, record_size=1000))

        update_datastream_after_produce = (UpdateDatastream(whitelist=None,
                                                            metadata=['system.auto.offset.reset:latest'],
                                                            name=datastream_name,
                                                            cluster=BrooklinClusterChoice.CONTROL),
                                           UpdateDatastream(whitelist=None,
                                                            metadata=['system.auto.offset.reset:latest'],
                                                            name=datastream_name,
                                                            cluster=BrooklinClusterChoice.EXPERIMENT))

        sleep_after_producing_traffic = Sleep(secs=60 * 5)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        consume_records = (ConsumeFromDestinationTopics(topics=control_topics, num_records=10000),
                           ConsumeFromDestinationTopics(topics=experiment_topics, num_records=10000))

        ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                      endtime_getter=sleep_until_test_end.end_time)

        kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[0].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                       AddDeferredKafkaAuditInquiry(test_name=self.testName,
                                                    starttime_getter=create_datastream[1].end_time,
                                                    endtime_getter=sleep_until_test_end.end_time,
                                                    topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_before_topics_creation)

        # If auto-topic creation is turned off and passthough is disabled, we rely on Kafka to create the topics,
        # which only happen when we produce to the destination topic. Even with the passthrough case, there could be
        # some rebalances which race with when we start producing, which can lead to accidentally skipping some events
        # if we haven't committed any checkpoints. To ensure we get all the data in the topics, we need set the
        # datastream's auto.offset.reset policy to 'earliest'.
        if not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*update_datastream_before_create)

        builder.add_parallel(*create_source_topics)

        # If passthrough is enabled but auto-topic creation isn't, we need to manually create topics on the destination
        # Kafka cluster. This mimics what we do in actual clusters with this configuration.
        if DatastreamConfigChoice.CONTROL.value.passthrough and not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*create_destination_topics) \
                .add_sequential(*wait_for_topic_on_destination)

        # If auto-topic creation is turned on, we wait for the destination topics to be created before we produce data.
        if DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_sequential(*wait_for_topic_on_destination)

        builder.add_parallel(*produce_traffic) \
            .add_sequential(sleep_after_producing_traffic, sleep_until_test_end)

        if not DatastreamConfigChoice.CONTROL.value.topic_create and not \
                DatastreamConfigChoice.CONTROL.value.passthrough:
            builder.add_sequential(*wait_for_topic_on_destination)

        if not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*update_datastream_after_produce)

        runner = builder.add_parallel(*consume_records) \
            .add_sequential(ekg_analysis, *kafka_audit) \
            .build()

        self.doRunTest(runner)


class BrooklinClusterBounceTests(KafkaAuditTestCaseBase):
    """All tests involving cluster restarts that use LID"""

    def test_brooklin_cluster_parallel_bounce(self):
        self.doRunTest(restart_brooklin_cluster(self.testName, 100))

    def test_brooklin_cluster_rolling_bounce(self):
        self.doRunTest(restart_brooklin_cluster(self.testName, 10))


class BrooklinErrorInducingTests(KafkaAuditTestCaseBase):
    """All Brooklin error-inducing certification tests"""

    def test_kill_random_brooklin_host(self):
        self.doRunTest(kill_start_brooklin_host(self.testName, False))

    def test_kill_leader_brooklin_host(self):
        self.doRunTest(kill_start_brooklin_host(self.testName, True))

    def test_stop_random_brooklin_host(self):
        self.doRunTest(stop_start_brooklin_host(self.testName, False))

    def test_stop_leader_brooklin_host(self):
        self.doRunTest(stop_start_brooklin_host(self.testName, True))

    def test_pause_resume_random_brooklin_host(self):
        self.doRunTest(pause_resume_brooklin_host(self.testName, False))

    def test_pause_resume_leader_brooklin_host(self):
        self.doRunTest(pause_resume_brooklin_host(self.testName, True))


class KafkaErrorInducingTests(KafkaAuditTestCaseBase):
    """All Kafka error-inducing certification tests"""

    def test_kill_random_source_kafka_broker(self):
        self.doRunTest(kill_kafka_broker(self.testName, KafkaClusterChoice.SOURCE))

    def test_kill_random_destination_kafka_broker(self):
        self.doRunTest(kill_kafka_broker(self.testName, KafkaClusterChoice.DESTINATION))

    def test_stop_random_source_kafka_broker(self):
        self.doRunTest(stop_kafka_broker(self.testName, KafkaClusterChoice.SOURCE))

    def test_stop_random_destination_kafka_broker(self):
        self.doRunTest(stop_kafka_broker(self.testName, KafkaClusterChoice.DESTINATION))

    def test_perform_ple_source_kafka_cluster(self):
        self.doRunTest(perform_kafka_ple(self.testName, KafkaClusterChoice.SOURCE))

    def test_perform_ple_destination_kafka_cluster(self):
        self.doRunTest(perform_kafka_ple(self.testName, KafkaClusterChoice.DESTINATION))

    def test_restart_source_kafka_cluster(self):
        self.doRunTest(restart_kafka_cluster(self.testName, KafkaClusterChoice.SOURCE))

    def test_restart_destination_kafka_cluster(self):
        self.doRunTest(restart_kafka_cluster(self.testName, KafkaClusterChoice.DESTINATION))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--audit-only', dest='audit_only', action='store_true')  # defaults to False
    args, rest = parser.parse_known_args()

    unittest.main(argv=sys.argv[:1] + rest, testLoader=CustomTestLoader(args.audit_only))
