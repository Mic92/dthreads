import os
import sys
import pwd
import grp
from threading import BrokenBarrierError

from . import Error


def drop_privileges(user, group):
    if os.getuid() != 0:
        raise Error("Must run as root to drop priviliges")

    if type(group) is int:
        os.setgid(group)
    elif group is not None:
        os.setgid(grp.getgrnam(group).gr_gid)

    # Remove group privileges
    if user is not None or group is not None:
        os.setgroups([])

    if type(user) is int:
        os.setuid(user)
    elif user is not None:
        os.setuid(pwd.getpwnam(user).pw_uid)


class Command():
    def __init__(self, tthread_path=None, user=None, group=None, cgroups=None):
        self.tthread_path = tthread_path
        self.user = user
        self.group = group
        self.cgroups = cgroups

    def exec(self, command, barrier):
        env = os.environ.copy()
        if self.tthread_path is not None:
            env["LD_PRELOAD"] = str(self.tthread_path)
            env["TTHREAD_NO_LOG"] = "1"
            env["TTHREAD_NO_MMAP_PROTECT"] = "1"
            env["LD_BIND_NOW"] = "1"
        try:
            barrier.wait(timeout=3)
        except BrokenBarrierError:
            print("Parent process timed out", file=sys.stderr)
        for cgroup in self.cgroups:
            cgroup.addPids(os.getpid())
        drop_privileges(self.user, self.group)
        os.execvpe(command[0], command, env)
