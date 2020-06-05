from typing import Type, Union

from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.teststeps import CreateDatastream
from testlib.core.runner import TestRunnerBuilder, TestRunner
from testlib.core.teststeps import Sleep, RestartCluster
from testlib.data import KafkaTopicFileChoice
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.environment import KafkaClusterChoice
from testlib.likafka.teststeps import KillRandomKafkaHost, StartKafkaHost, RunKafkaAudit, StopRandomKafkaHost, \
    PerformKafkaPreferredLeaderElection


def apply_revert_kafka_broker(datastream_name, kafka_cluster: KafkaClusterChoice,
                              apply_step_type: Type[Union[KillRandomKafkaHost, StopRandomKafkaHost]]) -> TestRunner:
    create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_apply = Sleep(secs=60 * 10)

    apply_kafka_broker = apply_step_type(cluster=kafka_cluster)

    sleep_after_apply = Sleep(secs=60)

    revert_kafka_broker = StartKafkaHost(apply_kafka_broker.get_host)

    sleep_after_revert = Sleep(secs=60 * 10)

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_after_revert.end_time)

    kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                 endtime_getter=sleep_after_revert.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                   RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                 endtime_getter=sleep_after_revert.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

    return TestRunnerBuilder(test_name=datastream_name) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_apply) \
        .add_sequential(apply_kafka_broker) \
        .add_sequential(sleep_after_apply) \
        .add_sequential(revert_kafka_broker) \
        .add_sequential(sleep_after_revert) \
        .add_sequential(ekg_analysis) \
        .add_parallel(*kafka_audit) \
        .build()


def kill_kafka_broker(datastream_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    return apply_revert_kafka_broker(datastream_name=datastream_name, kafka_cluster=kafka_cluster,
                                     apply_step_type=KillRandomKafkaHost)


def stop_kafka_broker(datastream_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    return apply_revert_kafka_broker(datastream_name=datastream_name, kafka_cluster=kafka_cluster,
                                     apply_step_type=StopRandomKafkaHost)


def perform_kafka_ple(datastream_name, kafka_cluster: KafkaClusterChoice) -> TestRunner:
    create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_ple = Sleep(secs=60 * 10)

    perform_ple = PerformKafkaPreferredLeaderElection(cluster=kafka_cluster)

    sleep_after_ple = Sleep(secs=60 * 10)

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_after_ple.end_time)

    kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                 endtime_getter=sleep_after_ple.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                   RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                 endtime_getter=sleep_after_ple.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

    return TestRunnerBuilder(test_name=datastream_name) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_ple) \
        .add_sequential(perform_ple) \
        .add_sequential(sleep_after_ple) \
        .add_sequential(ekg_analysis) \
        .add_parallel(*kafka_audit) \
        .build()


def restart_kafka_cluster(datastream_name, kafka_cluster: KafkaClusterChoice, host_concurrency) -> TestRunner:
    create_datastream = (CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL),
                         CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.EXPERIMENT))

    sleep_before_cluster_restart = Sleep(secs=60 * 10)

    restart_kafka = RestartCluster(cluster=kafka_cluster, host_concurrency=host_concurrency)

    sleep_after_cluster_restart = Sleep(secs=60 * 10)

    ekg_analysis = RunEkgAnalysis(starttime_getter=create_datastream[0].end_time,
                                  endtime_getter=sleep_after_cluster_restart.end_time)

    kafka_audit = (RunKafkaAudit(starttime_getter=create_datastream[0].end_time,
                                 endtime_getter=sleep_after_cluster_restart.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.VOYAGER),
                   RunKafkaAudit(starttime_getter=create_datastream[1].end_time,
                                 endtime_getter=sleep_after_cluster_restart.end_time,
                                 topics_file_choice=KafkaTopicFileChoice.EXPERIMENT_VOYAGER))

    return TestRunnerBuilder(test_name=datastream_name) \
        .add_parallel(*create_datastream) \
        .add_sequential(sleep_before_cluster_restart) \
        .add_sequential(restart_kafka) \
        .add_sequential(sleep_after_cluster_restart) \
        .add_sequential(ekg_analysis) \
        .add_parallel(*kafka_audit) \
        .build()
