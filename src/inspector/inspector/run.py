import os
import multiprocessing as mp
from . import cgroups, perf, tthread


def default_tthread_path():
    script_dir = os.path.dirname(__file__)
    tthread_dir = os.path.join(script_dir, "..", "..", "libtthread.so")
    return os.path.realpath(tthread_dir)


def run(command,
        tthread_path=default_tthread_path(),
        perf_command="perf",
        perf_log="perf.data",
        user=None,
        group=None,
        processor_trace=True,
        snapshot_mode=False,
        additional_cgroups=[]):

    cgroup_name = "inspector-%d" % os.getpid()

    perf_cgroup = cgroups.perf_event(cgroup_name)
    perf_cgroup.create()

    barrier = mp.Barrier(2)
    tthread_cmd = tthread.Command(tthread_path=tthread_path,
                                  user=user,
                                  group=group,
                                  cgroups=additional_cgroups + [perf_cgroup])
    process = mp.Process(target=tthread_cmd.exec,
                         args=(command, barrier,))
    process.start()

    return perf.run(perf_command,
                    perf_log,
                    barrier,
                    process,
                    perf_cgroup,
                    processor_trace=processor_trace,
                    snapshot_mode=snapshot_mode)
