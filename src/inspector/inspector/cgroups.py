import os
import signal
from . import Error


def find_mount(cgroup_type):
    try:
        with open("/proc/mounts", "r") as mounts:
            for line in mounts:
                fields = line.split(" ")
                if len(fields) < 6:
                    continue
                dev, mount, fs, opts, fs_freq, fs_passno = fields
                if fs == "cgroup" and cgroup_type in opts:
                    return mount
    except OSError as e:
        raise Error("Failed to open /proc/mounts: %s" % e)
    return None


def perf_events(name):
    return Group(name, "perf_events")


def cpu(name):
    return Group(name, "cpu")


class Group():
    def __init__(self, name, type_):
        mount = find_mount(type_)
        if mount is None:
            msg = "mount for %s cgroup not found in /proc/mounts." % type_
            if type_ == "perf_events":
                msg += " Has kernel CONFIG_PERF_EVENTS=y set?"
            raise Error(msg)
        self.name = name
        self.mountpoint = os.path.join(mount, name)

    def addPids(self, *pids):
        tasks = os.path.join(self.mountpoint, "tasks")
        try:
            with open(tasks, "a") as f:
                for pid in pids:
                    f.write("%s\n" % pid)
        except OSError as e:
            msg = "Failed to add process '%s' to cgroup '%s': %s" \
                  % (pid, self.mountpoint, e)
            raise Error(msg)

    def create(self):
        try:
            os.mkdir(self.mountpoint)
        except OSError as e:
            if e.errno == 17:  # file exists
                return
            msg = "Failed to create cgroup '%s': %s" % (self.mountpoint, e)
            raise Error(msg)

    def _move_processes(self, dest_cgroup):
        try:
            pids = []
            with open(os.path.join(self.mountpoint, "tasks")) as task:
                for line in task:
                    pids.append(line.strip())
            dest_cgroup.addPids(*pids)
        except OSError as e:
            msg = "Failed to move processes from cgroup '%s' to '%s': %s"\
                % (self.mountpoint, dest_cgroup.mountpoint, e)
            raise Error(msg)

    def destroy(self):
        self._move_processes(Group(""))

        with open(os.path.join(self.mountpoint, "tasks"), "r") as f:
            for line in f:
                os.kill(int(line), signal.SIGKILL)
        try:
            os.rmdir(self.mountpoint)
        except OSError as e:
            msg = "Failed to remove cgroup '%s': %s" % (self.mountpoint, e)
            raise Error(msg)

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, type, value, traceback):
        self.destroy()
