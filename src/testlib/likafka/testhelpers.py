from typing import Type, Union

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.teststeps import CreateDatastream
from testlib.core.runner import TestRunnerBuilder, TestRunner
from testlib.core.teststeps import Sleep, RestartCluster, SleepUntilNthMinute
from testlib.data import KafkaTopicFileChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.audit import AddDeferredKafkaAuditInquiry
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import KillRandomKafkaHost, StartKafkaHost, StopRandomKafkaHost, \
    PerformKafkaPreferredLeaderElection


def apply_revert_kafka_broker(test_name, kafka_cluster: KafkaClusterChoice,
                              apply_step_type: Type[Union[KillRandomKafkaHost, StopRandomKafkaHost]]) -> TestRunner:
    sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

    create_datastream = (CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_apply = Sleep(secs=60 * 10)

    apply_kafka_broker = apply_step_type(cluster=kafka_cluster)

    sleep_after_apply = Sleep(secs=60)

    revert_kafka_broker = StartKafkaHost(apply_kafka_broker.get_host)

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
        .add_sequential(sleep_before_apply, apply_kafka_broker, sleep_after_apply, revert_kafka_broker,
                        sleep_after_revert, sleep_until_test_end, ekg_analysis, *kafka_audit) \
        .build()


def kill_kafka_broker(test_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    return apply_revert_kafka_broker(test_name=test_name, kafka_cluster=kafka_cluster,
                                     apply_step_type=KillRandomKafkaHost)


def stop_kafka_broker(test_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    return apply_revert_kafka_broker(test_name=test_name, kafka_cluster=kafka_cluster,
                                     apply_step_type=StopRandomKafkaHost)


def perform_kafka_ple(test_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

    create_datastream = (CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_ple = Sleep(secs=60 * 10)

    perform_ple = PerformKafkaPreferredLeaderElection(cluster=kafka_cluster)

    sleep_after_ple = Sleep(secs=60 * 10)

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
        .add_sequential(sleep_before_ple, perform_ple, sleep_after_ple, sleep_until_test_end, ekg_analysis,
                        *kafka_audit) \
        .build()


def restart_kafka_cluster(test_name, kafka_cluster: KafkaClusterChoice, host_concurrency) -> TestRunner:
    sleep_until_test_start = SleepUntilNthMinute(nth_minute=7)

    create_datastream = (CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=test_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_cluster_restart = Sleep(secs=60 * 10)

    restart_kafka = RestartCluster(cluster=kafka_cluster, host_concurrency=host_concurrency)

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
        .add_sequential(sleep_before_cluster_restart, restart_kafka, sleep_after_cluster_restart, sleep_until_test_end,
                        ekg_analysis, *kafka_audit) \
        .build()
