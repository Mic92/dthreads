import os
import unittest
import tempfile
import inspector
from inspector import cgroups

TEST_ROOT = os.path.realpath(os.path.dirname(__file__))


class CgroupTest(unittest.TestCase):
    def test_create(self):
        with cgroups.perf_event("inspector-test") as c:
            pid = str(os.getpid())
            c.addPids(pid)
            f = open(os.path.join(c.mountpoint, "tasks"))
            line = f.readline()
            f.close()
            self.assertEqual(pid + "\n", line)


def perf_cmd():
    return os.getenv("PERF_COMMAND", "perf")


class PerfTest(unittest.TestCase):
    def test_run(self):
        sample_app = os.path.join(TEST_ROOT, "../../test/usage-test")

        with tempfile.NamedTemporaryFile() as log_file:
            process = inspector.run([sample_app],
                                    perf_command=perf_cmd(),
                                    perf_log=log_file.name)
            status = process.wait()
            self.assertEqual(0, status.exit_code)
            self.assertEqual(-15, status.perf_exit_code)  # SIGTERM == 15
            self.assertGreater(os.path.getsize(log_file.name), 1000)
            self.assertGreater(status.duration, 0)

    def test_run_without_pt(self):
        sample_app = os.path.join(TEST_ROOT, "../../test/usage-test")

        with tempfile.NamedTemporaryFile() as log_file:
            process = inspector.run([sample_app],
                                    perf_command=perf_cmd(),
                                    perf_log=log_file.name,
                                    processor_trace=False)
            status = process.wait()
            self.assertEqual(0, status.exit_code)

if __name__ == '__main__':
    unittest.main()
