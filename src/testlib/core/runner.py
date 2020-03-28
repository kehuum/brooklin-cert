import logging
import subprocess

from typing import Callable
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.brooklin.teststeps import KillBrooklinCluster, StartBrooklinCluster, PingBrooklinCluster
from testlib.core.teststeps import TestStep, NukeZooKeeper
from testlib.core.utils import typename


class TestRunner(object):
    """Main test driver responsible for running an end-to-end test"""

    def __init__(self, test_name):
        self.test_name = test_name

    def run(self, *steps: TestStep):
        pretest_steps = TestRunner._get_pretest_steps()
        self._run_steps(*pretest_steps, *steps)

    def _run_steps(self, *steps: TestStep):
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
    def _get_pretest_steps():
        # TODO: add steps for Brooklin's experiment cluster
        return PingBrooklinCluster(cluster=BrooklinClusterChoice.CONTROL),\
               KillBrooklinCluster(cluster=BrooklinClusterChoice.CONTROL),\
               NukeZooKeeper(cluster=BrooklinClusterChoice.CONTROL),\
               StartBrooklinCluster(cluster=BrooklinClusterChoice.CONTROL)
