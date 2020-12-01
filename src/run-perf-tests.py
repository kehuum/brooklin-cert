import logging
import unittest

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.brooklin.teststeps import CreateDatastream, UpdateDatastream
from testlib.core.runner import TestRunnerBuilder
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import ListTopics, CreateSourceTopicsOnDestination, \
    ValidateSourceAndDestinationTopicsMatch

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')
logging.getLogger('kafka').setLevel(logging.WARN)
logging.getLogger('kazoo').setLevel(logging.WARN)


class PerformanceTest(unittest.TestCase):
    """All performance test cases. These tests do not run audit since they rely on reading from topic start to push
    more load through the pipeline. This is added as a standalone TestCase because it can cause odd behavior with audit
    for tests run after this one, and it may take time for audit to recover and work as expected"""

    def test_performance(self):
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

        create_destination_topics = (CreateSourceTopicsOnDestination(topics_getter=list_topics_source[0].get_topics),
                                     CreateSourceTopicsOnDestination(topics_getter=list_topics_source[1].get_topics))

        datastream_name = 'test_performance'
        create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL,
                                              offset_reset='earliest', enable_cleanup=True),
                             CreateDatastream(name=datastream_name,
                                              datastream_config=DatastreamConfigChoice.EXPERIMENT,
                                              offset_reset='earliest', enable_cleanup=True))

        update_datastream = (UpdateDatastream(whitelist='^(^voyager-api.*$)|(^seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.CONTROL),
                             UpdateDatastream(whitelist='^(^experiment-voyager-api.*$)|(^experiment-seas-.*$)',
                                              metadata=['system.reuseExistingDestination:false'], name=datastream_name,
                                              cluster=BrooklinClusterChoice.EXPERIMENT))

        sleep_after_update = Sleep(secs=60 * 60)

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
                                      endtime_getter=sleep_after_update.end_time)

        builder = TestRunnerBuilder(test_name=datastream_name) \
            .add_parallel(*list_topics_source)

        # If passthrough is enabled but auto-topic creation isn't, we need to manually create topics on the destination
        # Kafka cluster. This mimics what we do in actual PROD clusters with this configuration.
        if DatastreamConfigChoice.CONTROL.value.passthrough and not DatastreamConfigChoice.CONTROL.value.topic_create:
            builder.add_parallel(*create_destination_topics)

        runner = builder.add_parallel(*create_datastream) \
            .add_parallel(*update_datastream) \
            .add_sequential(sleep_after_update) \
            .add_parallel(*list_topics_destination_after_update) \
            .add_parallel(*validate_topics_after_update) \
            .add_sequential(ekg_analysis) \
            .build()

        self.assertTrue(runner.run())


if __name__ == '__main__':
    unittest.main()
