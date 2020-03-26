from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import KillRandomKafkaHost, StartKafkaHost, RunKafkaAudit, StopRandomKafkaHost, \
    PerformKafkaPreferredLeaderElection, RestartKafkaCluster


def kill_kafka_broker(datastream_name, kafka_cluster):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_kill = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_kill)

    kill_kafka_host = KillRandomKafkaHost(cluster=kafka_cluster)
    test_steps.append(kill_kafka_host)

    # TODO: Add a step for hard killing a random Kafka host in the experiment cluster

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartKafkaHost(kill_kafka_host.get_host))

    # TODO: Add a step for starting the killed Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def stop_kafka_broker(datastream_name, kafka_cluster):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_stop = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_stop)

    stop_kafka_host = StopRandomKafkaHost(cluster=kafka_cluster)
    test_steps.append(stop_kafka_host)

    # TODO: Add a step for stopping the Brooklin host in the experiment cluster

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartKafkaHost(stop_kafka_host.get_host))

    # TODO: Add a step for starting the Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def perform_kafka_ple(datastream_name, kafka_cluster):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_kill = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_kill)

    perform_ple = PerformKafkaPreferredLeaderElection(cluster=kafka_cluster)
    test_steps.append(perform_ple)

    # TODO: Add a step for performing PLE in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def restart_kafka_cluster(datastream_name, kafka_cluster, host_concurrency):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_restart = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_restart)

    restart_kafka = RestartKafkaCluster(cluster=kafka_cluster, host_concurrency=host_concurrency)
    test_steps.append(restart_kafka)

    sleep_after_restart = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_restart)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_restart.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_restart.end_time))
    return test_steps
