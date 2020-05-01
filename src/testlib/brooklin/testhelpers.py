from typing import Type, Union

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, KillRandomBrooklinHost, \
    StartBrooklinHost, StopRandomBrooklinHost, PauseRandomBrooklinHost, ResumeBrooklinHost, KillLeaderBrooklinHost, \
    StopLeaderBrooklinHost, PauseLeaderBrooklinHost
from testlib.core.runner import TestRunnerBuilder, TestRunner
from testlib.core.teststeps import Sleep, RestartCluster
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit


def apply_revert_brooklin_host(
        datastream_name, is_leader,
        apply_random_step_type: Type[Union[KillRandomBrooklinHost, StopRandomBrooklinHost, PauseRandomBrooklinHost]],
        apply_leader_step_type: Type[Union[KillLeaderBrooklinHost, StopLeaderBrooklinHost, PauseLeaderBrooklinHost]],
        revert_step_type: Type[Union[StartBrooklinHost, ResumeBrooklinHost]]) -> TestRunner:

    create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_apply = Sleep(secs=60 * 10)

    apply_step_type = apply_leader_step_type if is_leader else apply_random_step_type
    apply_brooklin_host = (apply_step_type(cluster=BrooklinClusterChoice.CONTROL),
                           apply_step_type(cluster=BrooklinClusterChoice.EXPERIMENT))

    sleep_after_apply = Sleep(secs=60)

    revert_brooklin_host = (revert_step_type(apply_brooklin_host[0].get_host),
                            revert_step_type(apply_brooklin_host[1].get_host))

    sleep_after_revert = Sleep(secs=60 * 10)

    kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                 endtime_getter=sleep_after_apply.end_time,
                                 topics_file='data/voyager-topics.txt'),
                   RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                 endtime_getter=sleep_after_apply.end_time,
                                 topics_file='data/experiment-voyager-topics.txt'))

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_after_apply.end_time)

    return TestRunnerBuilder(test_name=datastream_name) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_apply) \
        .add_parallel(*apply_brooklin_host) \
        .add_sequential(sleep_after_apply) \
        .add_parallel(*revert_brooklin_host) \
        .add_sequential(sleep_after_revert) \
        .add_parallel(*kafka_audit) \
        .add_sequential(ekg_analysis) \
        .build()


def kill_start_brooklin_host(datastream_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(
        datastream_name, is_leader, apply_leader_step_type=KillLeaderBrooklinHost,
        apply_random_step_type=KillRandomBrooklinHost, revert_step_type=StartBrooklinHost)


def stop_start_brooklin_host(datastream_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(
        datastream_name, is_leader, apply_leader_step_type=StopLeaderBrooklinHost,
        apply_random_step_type=StopRandomBrooklinHost, revert_step_type=StartBrooklinHost)


def pause_resume_brooklin_host(datastream_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(
        datastream_name, is_leader, apply_leader_step_type=PauseLeaderBrooklinHost,
        apply_random_step_type=PauseRandomBrooklinHost, revert_step_type=ResumeBrooklinHost)


def restart_brooklin_cluster(datastream_name, host_concurrency):
    test_steps = []
    control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_restart = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_restart)

    restart_brooklin = RestartCluster(cluster=BrooklinClusterChoice.CONTROL, host_concurrency=host_concurrency)
    test_steps.append(restart_brooklin)

    # TODO: Add a step for restarting the the experiment cluster

    sleep_after_restart = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_restart)

    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_restart.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_restart.end_time))
    return test_steps
