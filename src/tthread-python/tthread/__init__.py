import os
import tempfile
import subprocess


class Error(Exception):
    pass

# needs to be defined after Error, because log.Error inherits from this
from tthread import accesslog  # noqa


def default_library_path():
    script_dir = os.path.dirname(__file__)
    tthread_dir = os.path.join(script_dir, "..", "..", "libtthread.so")
    return os.path.realpath(tthread_dir)


class Process:
    def __init__(self, popen, log_file):
        self.popen = popen
        self.log_file = log_file

    def wait(self):
        return accesslog.Log(self.popen.wait(), self.log_file)


def run(command,
        tthread_path=default_library_path(),
        stdin=None,
        stdout=None,
        stderr=None):
    log_file = tempfile.TemporaryFile()
    log_fd = log_file.fileno()
    pass_fds = [0, 1, 2, log_fd]
    env = os.environ.copy()
    env["LD_PRELOAD"] = tthread_path
    env["TTHREAD_LOG_FD"] = str(log_fd)
    env["LD_BIND_NOW"] = "1"
    popen = subprocess.Popen(command,
                             pass_fds=pass_fds,
                             env=env,
                             stdin=stdin,
                             stdout=stdout,
                             stderr=stderr)
    return Process(popen, log_file)
