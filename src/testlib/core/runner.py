import concurrent
import logging
import subprocess

from concurrent.futures.process import ProcessPoolExecutor
from itertools import chain
from operator import methodcaller, itemgetter
from typing import Iterable, List, Union, Callable
from testlib.brooklin.environment import BrooklinClusterChoice
from testlib.brooklin.teststeps import KillBrooklinCluster, StartBrooklinCluster, PingBrooklinCluster
from testlib.core.teststeps import TestStep, NukeZooKeeper, ParallelTestStepGroup
from testlib.core.utils import typename

TestStepOrParallelTestStepGroup = Union[TestStep, ParallelTestStepGroup]


class TestRunnerBuilder(object):
    """A builder for TestRunner objects"""

    def __init__(self, test_name):
        self._test_name = test_name
        self._steps: List[TestStepOrParallelTestStepGroup] = []

    def add_sequential(self, *steps: TestStep):
        self._steps.extend(steps)
        return self

    def add_parallel(self, *steps: TestStep):
        self._steps.append(ParallelTestStepGroup(*steps))
        return self

    def build(self):
        return TestRunner(self._test_name, chain(TestRunnerBuilder._get_pretest_steps(), self._steps))

    @staticmethod
    def _get_pretest_steps():
        steps = []
        for cluster in (BrooklinClusterChoice.CONTROL, BrooklinClusterChoice.EXPERIMENT):
            steps += [PingBrooklinCluster(cluster=cluster),
                      KillBrooklinCluster(cluster=cluster, skip_if_dead=True),
                      NukeZooKeeper(cluster=cluster),
                      StartBrooklinCluster(cluster=cluster)]
        return steps


class TestRunner(object):
    """Main test driver responsible for running an end-to-end test"""

    def __init__(self, test_name: str, steps: Iterable[TestStepOrParallelTestStepGroup]):
        self._test_name = test_name
        self._steps = steps

    def run(self):
        return self._run_steps(self._steps)

    def _run_steps(self, steps: Iterable[TestStepOrParallelTestStepGroup]):
        test_success = True
        cleanup_steps = []

        for s in steps:
            step_name = f'{self._test_name}:{typename(s)}'
            cleanup_steps.append(s)

            logging.info(f'Running test step {step_name}')
            success, message = self._execute(s)
            if not success:
                logging.error(f'Test step {step_name} failed with an error: {message}')
                test_success = False
                break

        for c in reversed(cleanup_steps):
            step_name = f'{self._test_name}:{typename(c)}'
            logging.info(f'Running cleanup test step {step_name}')
            success, message = self._cleanup(c)
            if not success:
                logging.error(f'Cleanup test step {step_name} failed with an error: {message}')
                test_success = False

        return test_success

    @staticmethod
    def _execute(s: TestStepOrParallelTestStepGroup):
        run_methodcaller = methodcaller('run')
        return TestRunner._call(s, run_methodcaller)

    @staticmethod
    def _cleanup(s: TestStepOrParallelTestStepGroup):
        cleanup_methodcaller = methodcaller('cleanup')
        return TestRunner._call(s, cleanup_methodcaller)

    @staticmethod
    def _call(s: TestStepOrParallelTestStepGroup, fn_caller: Callable[[TestStep], None]) -> (bool, str):
        if isinstance(s, ParallelTestStepGroup):
            return TestRunner._parallel_execute_step_group_fns(step_group=s, fn_caller=fn_caller)
        return TestRunner._execute_step_fn(step=s, fn_caller=fn_caller)[:2]

    @staticmethod
    def _execute_step_fn(step: TestStep, fn_caller: Callable[[TestStep], None]) -> (bool, str, TestStep):
        """Expects a TestStep and a caller that calls a function on that TestStep,
        typically TestStep.run() or TestStep.cleanup().

        It returns:
            - A boolean that indicates whether or not the called function executed
            without raising exceptions

            - An error message in case the called function raised an exception

            - The passed TestStep object. This is only useful for scenarios where
            _execute_step_fn() is executed in a different process (e.g. in the case
            of parallel steps). Passing the TestStep object back allows the calling
            process to observe any side-effects caused by executing the TestStep
            function (e.g. changes in TestStep.start_time(), TestStep.end_time())
        """
        try:
            fn_caller(step)  # execute the specified function
        except Exception as err:
            error_message = [f'Test step {typename(step)} failed with error: {err}']
            if isinstance(err, subprocess.CalledProcessError):
                if err.stdout:
                    error_message += [f'stdout:\n{err.stdout.strip()}']
                if err.stderr:
                    error_message += [f'stderr:\n{err.stderr.strip()}']
            return False, '\n'.join(error_message), step
        else:
            return True, '', step

    @staticmethod
    def _parallel_execute_step_group_fns(step_group: ParallelTestStepGroup,
                                         fn_caller: Callable[[TestStep], None]) -> (bool, str):
        n = len(step_group.steps)

        with ProcessPoolExecutor(max_workers=n) as executor:
            futures = [executor.submit(TestRunner._execute_step_fn, s, fn_caller) for s in step_group.steps]
            done, not_done = concurrent.futures.wait(futures)  # waits indefinitely for all steps to fail/finish
            results = [future.result() for future in done]

        status_getter, message_getter, step_getter = [itemgetter(i) for i in range(3)]

        # If one step fails, the entire group is considered failed
        if not_done or not all(status_getter(r) for r in results):
            error_message = "\n".join(message_getter(r) for r in results if not status_getter(r))
            return False, f'Executing one or more parallel test steps failed: {error_message}'

        # Because step_group.steps are executed in parallel in multiple independent processes,
        # any changes made to them in those external processes are not reflected in this process.
        # To address this, the code below copies all the fields of the executed TestSteps back to
        # step_group.steps.
        steps_by_id = {step_getter(r).id: step_getter(r) for r in results}
        for s in step_group.steps:
            s.__dict__ = steps_by_id[s.id].__dict__

        return True, ''
