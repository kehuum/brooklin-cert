import argparse
import logging
import sys
import unittest

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.brooklin.teststeps import CreateDatastreamWithElasticTaskAssignmentEnabled
from testlib.core.loader import CustomTestLoader
from testlib.core.runner import TestRunnerBuilder
from testlib.core.teststeps import Sleep, SleepUntilNthMinute, GetAndValidateNumTasksFromZooKeeper
from testlib.data import KafkaTopicFileChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.audit import AddDeferredKafkaAuditInquiry, KafkaAuditTestCaseBase
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import ListTopics, GetTotalPartitionCountForSourceTopics, CreateTopics, \
    ValidateDestinationTopicsExist, ProduceToSourceTopics

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')
logging.getLogger('kafka').setLevel(logging.WARN)
logging.getLogger('kazoo').setLevel(logging.WARN)


class ElasticTaskAssignmentTest(KafkaAuditTestCaseBase):
    """All elastic task assignment test cases. These tests should only be run for BMM partition-managed variants.
    This is added as a standalone test since it will only work for partition-managed variants, whereas the other
    test cases work for all variants of BMM."""

    def test_elastic_task_assignment(self):
        control_topic_prefixes = ['voyager-api-']
        experiment_topic_prefixes = ['experiment-voyager-api-']

        list_topics_source = \
            (ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=control_topic_prefixes),
             ListTopics(cluster=KafkaClusterChoice.SOURCE, topic_prefixes_filter=experiment_topic_prefixes))

        get_partition_count = (GetTotalPartitionCountForSourceTopics(topics_getter=list_topics_source[0].get_topics),
                               GetTotalPartitionCountForSourceTopics(topics_getter=list_topics_source[1].get_topics))

        sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

        datastream_name = 'test_elastic_task_assignment'
        create_datastream = \
            (CreateDatastreamWithElasticTaskAssignmentEnabled(name=datastream_name,
                                                              datastream_config=DatastreamConfigChoice.CONTROL,
                                                              partition_count_getter=get_partition_count[0].get_partition_count,
                                                              partitions_per_task=100, fullness_factor_pct=75,
                                                              offset_reset='earliest', enable_cleanup=True),
             CreateDatastreamWithElasticTaskAssignmentEnabled(name=datastream_name,
                                                              datastream_config=DatastreamConfigChoice.EXPERIMENT,
                                                              partition_count_getter=get_partition_count[1].get_partition_count,
                                                              partitions_per_task=100, fullness_factor_pct=75,
                                                              offset_reset='earliest', enable_cleanup=True))

        sleep_after_create = Sleep(secs=10 * 60)

        get_and_validate_num_tasks_after_create = \
            (GetAndValidateNumTasksFromZooKeeper(cluster=BrooklinClusterChoice.CONTROL,
                                                 datastream_name=datastream_name,
                                                 expected_num_tasks_getter=create_datastream[0].get_expected_num_tasks),
             GetAndValidateNumTasksFromZooKeeper(cluster=BrooklinClusterChoice.EXPERIMENT,
                                                 datastream_name=datastream_name,
                                                 expected_num_tasks_getter=create_datastream[1].get_expected_num_tasks))

        control_topics = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]
        experiment_topics = [f'experiment-voyager-api-bmm-certification-test-{i}' for i in range(10)]

        create_source_topics = (CreateTopics(topics=control_topics, cluster=KafkaClusterChoice.SOURCE,
                                             delay_seconds=0, partitions=20),
                                CreateTopics(topics=experiment_topics, cluster=KafkaClusterChoice.SOURCE,
                                             delay_seconds=0, partitions=20))

        create_destination_topics = (CreateTopics(topics=control_topics, cluster=KafkaClusterChoice.DESTINATION,
                                                  delay_seconds=0, partitions=20),
                                     CreateTopics(topics=experiment_topics, cluster=KafkaClusterChoice.DESTINATION,
                                                  delay_seconds=0, partitions=20))

        wait_for_topic_on_destination = (ValidateDestinationTopicsExist(topics=control_topics),
                                         ValidateDestinationTopicsExist(topics=experiment_topics))

        produce_traffic = (ProduceToSourceTopics(topics=control_topics, num_records=10000, record_size=1000),
                           ProduceToSourceTopics(topics=experiment_topics, num_records=10000, record_size=1000))

        sleep_after_produce = Sleep(secs=10 * 60)

        sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

        get_and_validate_num_tasks = \
            (GetAndValidateNumTasksFromZooKeeper(cluster=BrooklinClusterChoice.CONTROL,
                                                 datastream_name=datastream_name,
                                                 expected_num_tasks_getter=create_datastream[0].get_expected_num_tasks),
             GetAndValidateNumTasksFromZooKeeper(cluster=BrooklinClusterChoice.EXPERIMENT,
                                                 datastream_name=datastream_name,
                                                 expected_num_tasks_getter=create_datastream[1].get_expected_num_tasks))

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
            .add_parallel(*list_topics_source) \
            .add_parallel(*get_partition_count) \
            .add_sequential(sleep_until_test_start) \
            .add_parallel(*create_datastream) \
            .add_sequential(sleep_after_create) \
            .add_parallel(*get_and_validate_num_tasks_after_create) \
            .add_parallel(*create_source_topics) \
            .add_parallel(*create_destination_topics) \
            .add_sequential(*wait_for_topic_on_destination) \
            .add_parallel(*produce_traffic) \
            .add_sequential(sleep_after_produce) \
            .add_parallel(*get_and_validate_num_tasks) \
            .add_sequential(sleep_until_test_end, ekg_analysis, *kafka_audit) \
            .build()

        self.doRunTest(runner)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--audit-only', dest='audit_only', action='store_true')  # defaults to False
    args, rest = parser.parse_known_args()

    unittest.main(argv=sys.argv[:1] + rest, testLoader=CustomTestLoader(args.audit_only))
