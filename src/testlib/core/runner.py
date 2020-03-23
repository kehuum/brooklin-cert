import logging
import subprocess

from typing import Callable
from kazoo.client import KazooClient
from testlib.brooklin.environment import BrooklinClusterChoice, BrooklinDeploymentInfo
from testlib.core.utils import typename
from testlib.core.teststeps import TestStep


class TestRunner(object):
    """Main test driver responsible for running an end-to-end test"""

    def __init__(self, test_name):
        self.test_name = test_name

    def run(self, *steps: TestStep):
        test_success = True
        cleanup_steps = []

        for s in steps:
            step_name = f'{self.test_name}:{typename(s)}'
            cleanup_steps.append(s)

            logging.info(f'Running test step {step_name}')
            success, message = self.execute(s)
            if not success:
                logging.error(f'Test step {step_name} failed with an error: {message}')
                test_success = False
                break

        for c in reversed(cleanup_steps):
            step_name = f'{self.test_name}:{typename(c)}'
            logging.info(f'Running cleanup test step {step_name}')
            success, message = self.cleanup(c)
            if not success:
                logging.error(f'Cleanup test step {step_name} failed with an error: {message}')
                test_success = False

        return test_success

    @staticmethod
    def execute(step: TestStep):
        return TestRunner.execute_fn(fn=step.run, step_name=typename(step))

    @staticmethod
    def cleanup(step: TestStep):
        return TestRunner.execute_fn(fn=step.cleanup, step_name=typename(step))

    @staticmethod
    def execute_fn(fn: Callable[[], None], step_name) -> (bool, str):
        try:
            fn()
        except Exception as err:
            error_message = [f'Test step {step_name} failed with error: {err}']
            if isinstance(err, subprocess.CalledProcessError):
                if err.stdout:
                    error_message += [f'stdout:\n{err.stdout.strip()}']
                if err.stderr:
                    error_message += [f'stderr:\n{err.stderr.strip()}']
            return False, '\n'.join(error_message)
        else:
            return True, ''

    @staticmethod
    def __nuke_zookeeper(cluster: BrooklinClusterChoice):
        """Deletes everything under the root ZooKeeper znode of a Brooklin cluster

        This utility function is defined on this class to limit its visibility.

        WARNING: This function must be used with absolute caution since it deletes
                 all ZooKeeper nodes under the root znode of the specified Brooklin
                 cluster. It is also necessary to make sure the Brooklin cluster in
                 question is stopped before calling this function.
        """
        brooklin_cluster: BrooklinDeploymentInfo = cluster.value
        zk_client = KazooClient(hosts=brooklin_cluster.zk_dns)

        try:
            zk_client.start()
            root_znode = brooklin_cluster.zk_root_znode
            for child_znode in zk_client.get_children(root_znode):
                zk_client.delete(f'{root_znode}/{child_znode}', recursive=True)
        finally:
            zk_client.stop()
            zk_client.close()
