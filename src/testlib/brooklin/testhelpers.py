from typing import Type, Union

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, KillRandomBrooklinHost, \
    StartBrooklinHost, StopRandomBrooklinHost, PauseRandomBrooklinHost, ResumeBrooklinHost, KillLeaderBrooklinHost, \
    StopLeaderBrooklinHost, PauseLeaderBrooklinHost
from testlib.core.runner import TestRunnerBuilder, TestRunner
from testlib.core.teststeps import Sleep, RestartCluster, SleepUntilNthMinute
from testlib.data import KafkaTopicFileChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.audit import AddDeferredKafkaAuditInquiry


def apply_revert_brooklin_host(
        test_name, is_leader,
        apply_random_step_type: Type[Union[KillRandomBrooklinHost, StopRandomBrooklinHost, PauseRandomBrooklinHost]],
        apply_leader_step_type: Type[Union[KillLeaderBrooklinHost, StopLeaderBrooklinHost, PauseLeaderBrooklinHost]],
        revert_step_type: Type[Union[StartBrooklinHost, ResumeBrooklinHost]]) -> TestRunner:
    sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

    create_datastream = (CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_apply = Sleep(secs=60 * 10)

    apply_step_type = apply_leader_step_type if is_leader else apply_random_step_type
    apply_brooklin_host = (apply_step_type(cluster=BrooklinClusterChoice.CONTROL),
                           apply_step_type(cluster=BrooklinClusterChoice.EXPERIMENT))

    sleep_after_apply = Sleep(secs=60)

    revert_brooklin_host = (revert_step_type(apply_brooklin_host[0].get_host),
                            revert_step_type(apply_brooklin_host[1].get_host))

    sleep_after_revert = Sleep(secs=60 * 10)

    sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_until_test_end.end_time)

    kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=test_name,
                                                starttime_getter=create_datastream[0].end_time,
                                                endtime_getter=sleep_until_test_end.end_time,
                                                topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                   AddDeferredKafkaAuditInquiry(test_name=test_name,
                                                starttime_getter=create_datastream[1].end_time,
                                                endtime_getter=sleep_until_test_end.end_time,
                                                topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

    return TestRunnerBuilder(test_name=test_name) \
        .add_sequential(sleep_until_test_start) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_apply) \
        .add_parallel(*apply_brooklin_host) \
        .add_sequential(sleep_after_apply) \
        .add_parallel(*revert_brooklin_host) \
        .add_sequential(sleep_after_revert, sleep_until_test_end, ekg_analysis, *kafka_audit) \
        .build()


def kill_start_brooklin_host(test_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(test_name, is_leader, apply_leader_step_type=KillLeaderBrooklinHost,
                                      apply_random_step_type=KillRandomBrooklinHost, revert_step_type=StartBrooklinHost)


def stop_start_brooklin_host(test_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(test_name, is_leader, apply_leader_step_type=StopLeaderBrooklinHost,
                                      apply_random_step_type=StopRandomBrooklinHost, revert_step_type=StartBrooklinHost)


def pause_resume_brooklin_host(datastream_name, is_leader) -> TestRunner:
    return apply_revert_brooklin_host(
        datastream_name, is_leader, apply_leader_step_type=PauseLeaderBrooklinHost,
        apply_random_step_type=PauseRandomBrooklinHost, revert_step_type=ResumeBrooklinHost)


def restart_brooklin_cluster(test_name, host_concurrency) -> TestRunner:
    sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

    create_datastream = (CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_cluster_restart = Sleep(secs=60 * 10)

    restart_brooklin = (RestartCluster(cluster=BrooklinClusterChoice.CONTROL, host_concurrency=host_concurrency),
                        RestartCluster(cluster=BrooklinClusterChoice.EXPERIMENT, host_concurrency=host_concurrency))

    sleep_after_cluster_restart = Sleep(secs=60 * 10)

    sleep_until_test_end = SleepUntilNthMinute(nth_minute=0)

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_until_test_end.end_time)

    kafka_audit = (AddDeferredKafkaAuditInquiry(test_name=test_name,
                                                starttime_getter=create_datastream[0].end_time,
                                                endtime_getter=sleep_until_test_end.end_time,
                                                topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                   AddDeferredKafkaAuditInquiry(test_name=test_name,
                                                starttime_getter=create_datastream[1].end_time,
                                                endtime_getter=sleep_until_test_end.end_time,
                                                topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

    return TestRunnerBuilder(test_name=test_name) \
        .add_sequential(sleep_until_test_start) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_cluster_restart) \
        .add_parallel(*restart_brooklin) \
        .add_sequential(sleep_after_cluster_restart, sleep_until_test_end, ekg_analysis, *kafka_audit) \
        .build()
