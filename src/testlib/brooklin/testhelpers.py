from testlib.brooklin.datastream import DatastreamConfigChoice
from testlib.brooklin.teststeps import CreateDatastream, BrooklinClusterChoice, KillRandomBrooklinHost, \
    StartBrooklinHost, StopRandomBrooklinHost, PauseRandomBrooklinHost, ResumeBrooklinHost, KillLeaderBrooklinHost, \
    StopLeaderBrooklinHost, PauseLeaderBrooklinHost
from testlib.core.teststeps import Sleep, RestartCluster
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit


def kill_start_brooklin_host(datastream_name, is_leader):
    test_steps = []
    control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_kill = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_kill)

    if is_leader:
        kill_brooklin_host = KillLeaderBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(kill_brooklin_host)

        # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

    else:
        kill_brooklin_host = KillRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(kill_brooklin_host)

        # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartBrooklinHost(kill_brooklin_host.get_host))

    # TODO: Add a step for starting the killed Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)

    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def stop_start_brooklin_host(datastream_name, is_leader):
    test_steps = []
    control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_stop = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_stop)

    if is_leader:
        stop_brooklin_host = StopLeaderBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(stop_brooklin_host)

        # TODO: Add a step for stopping the leader Brooklin host in the experiment cluster

    else:
        stop_brooklin_host = StopRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(stop_brooklin_host)

        # TODO: Add a step for stopping the Brooklin host in the experiment cluster

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartBrooklinHost(stop_brooklin_host.get_host))

    # TODO: Add a step for starting the Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)

    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def pause_resume_brooklin_host(datastream_name, is_leader):
    test_steps = []
    control_datastream = CreateDatastream(name=datastream_name, datastream_config=DatastreamConfigChoice.CONTROL)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_stop = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_stop)

    if is_leader:
        pause_brooklin_host = PauseLeaderBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(pause_brooklin_host)

        # TODO: Add a step for pausing the leader Brooklin host in the experiment cluster

    else:
        pause_brooklin_host = PauseRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(pause_brooklin_host)

        # TODO: Add a step for pausing the Brooklin host in the experiment cluster

    test_steps.append(Sleep(secs=60))
    test_steps.append(ResumeBrooklinHost(pause_brooklin_host.get_host))

    # TODO: Add a step for resuming the Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)

    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


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
