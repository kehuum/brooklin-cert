#!/usr/bin/env python3

import logging
import unittest

from testlib.core.runner import TestRunnerBuilder
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import ListTopics, DeleteTopics

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


class PostTestCleanup(unittest.TestCase):
    """Cleanup to be performed after all tests have completed running"""

    def test_post_test_cleanup(self):
        test_steps = []

        seas_topics = ListTopics(cluster=KafkaClusterChoice.DESTINATION,
                                 topic_prefixes_filter=['seas-', 'experiment-seas-'])
        test_steps.append(seas_topics)
        test_steps.append(DeleteTopics(topics_getter=seas_topics.get_topics, cluster=KafkaClusterChoice.DESTINATION))

        created_topics = [f'voyager-api-bmm-certification-test-{i}' for i in range(10)]
        created_topics.extend([f'experiment-voyager-api-bmm-certification-test-{i}' for i in range(10)])

        def get_created_topics():
            return created_topics

        test_steps.append(DeleteTopics(topics_getter=get_created_topics, cluster=KafkaClusterChoice.DESTINATION,
                                       skip_on_failure=True))

        self.assertTrue(TestRunnerBuilder('test_post_test_cleanup')
                        .add_sequential(*test_steps)
                        .build().run())


if __name__ == '__main__':
    unittest.main()
