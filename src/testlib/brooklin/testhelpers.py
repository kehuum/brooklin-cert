from testlib.brooklin.teststeps import CreateDatastream, GetBrooklinLeaderHost, BrooklinClusterChoice, \
    KillBrooklinHost, KillRandomBrooklinHost, StartBrooklinHost, StopBrooklinHost, StopRandomBrooklinHost, \
    PauseBrooklinHost, PauseRandomBrooklinHost, ResumeBrooklinHost
from testlib.core.teststeps import Sleep
from testlib.ekg import RunEkgAnalysis
from testlib.likafka.teststeps import RunKafkaAudit


def kill_brooklin_host(datastream_name, is_leader):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_kill = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_kill)

    if is_leader:
        find_leader_host = GetBrooklinLeaderHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(find_leader_host)

        # TODO: Add a step for finding the leader Brooklin host in the experiment cluster

        kill_brooklin_host = KillBrooklinHost(hostname_getter=find_leader_host.get_leader_host)
        test_steps.append(kill_brooklin_host)

        # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

        host_getter = find_leader_host.get_leader_host
    else:
        kill_brooklin_host = KillRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(kill_brooklin_host)

        # TODO: Add a step for hard killing a random Brooklin host in the experiment cluster

        host_getter = kill_brooklin_host.get_host

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartBrooklinHost(host_getter))

    # TODO: Add a step for starting the killed Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps


def stop_brooklin_host(datastream_name, is_leader):
    test_steps = []
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_stop = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_stop)

    if is_leader:
        find_leader_host = GetBrooklinLeaderHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(find_leader_host)

        # TODO: Add a step for finding the leader Brooklin host in the experiment cluster

        stop_brooklin_host = StopBrooklinHost(hostname_getter=find_leader_host.get_leader_host)
        test_steps.append(stop_brooklin_host)

        # TODO: Add a step for stopping the leader Brooklin host in the experiment cluster

        host_getter = find_leader_host.get_leader_host
    else:
        stop_brooklin_host = StopRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(stop_brooklin_host)

        # TODO: Add a step for stopping the Brooklin host in the experiment cluster

        host_getter = stop_brooklin_host.get_host

    test_steps.append(Sleep(secs=60))
    test_steps.append(StartBrooklinHost(host_getter))

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
    control_datastream = CreateDatastream(cluster=BrooklinClusterChoice.CONTROL, name=datastream_name,
                                          topic_create=True, identity=False, passthrough=False, partition_managed=True)
    test_steps.append(control_datastream)

    # TODO: Add a step for creating experiment datastream

    sleep_before_stop = Sleep(secs=60 * 10)
    test_steps.append(sleep_before_stop)

    if is_leader:
        find_leader_host = GetBrooklinLeaderHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(find_leader_host)

        # TODO: Add a step for finding the leader Brooklin host in the experiment cluster

        pause_brooklin_host = PauseBrooklinHost(hostname_getter=find_leader_host.get_leader_host)
        test_steps.append(pause_brooklin_host)

        # TODO: Add a step for pausing the leader Brooklin host in the experiment cluster

        host_getter = find_leader_host.get_leader_host
    else:
        pause_brooklin_host = PauseRandomBrooklinHost(cluster=BrooklinClusterChoice.CONTROL)
        test_steps.append(pause_brooklin_host)

        # TODO: Add a step for pausing the Brooklin host in the experiment cluster

        host_getter = pause_brooklin_host.get_host

    test_steps.append(Sleep(secs=60))
    test_steps.append(ResumeBrooklinHost(host_getter))

    # TODO: Add a step for resuming the Brooklin host in the experiment cluster

    sleep_after_start = Sleep(secs=60 * 10)
    test_steps.append(sleep_after_start)
    test_steps.append(RunKafkaAudit(starttime_getter=control_datastream.end_time,
                                    endtime_getter=sleep_after_start.end_time))

    # TODO: Add a step for running audit on the experiment data-flow

    test_steps.append(RunEkgAnalysis(starttime_getter=control_datastream.end_time,
                                     endtime_getter=sleep_after_start.end_time))
    return test_steps
