#!/usr/bin/env python3

import logging
import unittest

from testlib.core.runner import TestRunnerBuilder
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import ListTopics, DeleteTopics

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')
logging.getLogger('kafka').setLevel(logging.WARN)
logging.getLogger('kazoo').setLevel(logging.WARN)


class PostTestCleanup(unittest.TestCase):
    """Cleanup to be performed after all tests have completed running"""

    def test_post_test_cleanup(self):
        list_topics = ListTopics(cluster=KafkaClusterChoice.DESTINATION,
                                 topic_prefixes_filter=['seas-', 'experiment-seas-'])
        delete_whitelisted_topics = DeleteTopics(topics_getter=list_topics.get_topics,
                                                 cluster=KafkaClusterChoice.DESTINATION)

        created_topics = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]
        created_topics += [f'experiment-voyager-api-bmm-certification-test-{i}' for i in range(10)]

        def get_created_topics():
            return created_topics

        delete_created_topics = DeleteTopics(topics_getter=get_created_topics, cluster=KafkaClusterChoice.DESTINATION,
                                             skip_on_failure=True)

        self.assertTrue(TestRunnerBuilder('test_post_test_cleanup')
                        .skip_pretest_steps()
                        .add_sequential(list_topics)
                        .add_sequential(delete_whitelisted_topics)
                        .add_sequential(delete_created_topics)
                        .build().run())


if __name__ == '__main__':
    unittest.main()
