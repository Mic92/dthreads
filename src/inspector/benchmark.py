import os
import sys
import argparse
import json
import subprocess
import inspector
import signal
from inspector import cgroups
if sys.version_info >= (3, 3):
    from shlex import quote
else:
    from pipes import quote

SCRIPT_ROOT = os.path.dirname(os.path.realpath(__file__))
EVAL_ROOT = os.path.realpath(os.path.join(SCRIPT_ROOT, "../../eval"))
TOTAL_THREADS = os.system("nproc --all")
TEST_PATH = os.path.join(EVAL_ROOT, "tests")
DATASET_HOME = os.path.join(EVAL_ROOT, "datasets")


class NCores:
    def to_param(self, cores):
        return cores


class CannealThreads(NCores):
    def to_param(self, cores):
        if cores == 16:
            return 15
        if cores == 8:
            return 7
        elif cores == 4:
            return 3
        else:  # == 2
            return 1


class DedupThreads(NCores):
    def to_param(self, cores):
        if cores == 8:
            return 2
        else:  # cores == 4 or cores == 2
            return 1


def set_online_cpus(threads=TOTAL_THREADS, verbose=True):
    for i in list(range(1, TOTAL_THREADS - 1)):
        enable = (i % int(TOTAL_THREADS / threads)) == 0
        with open("/sys/devices/system/cpu/cpu%d/online" % i, "w") as f:
            if enable:
                f.write("1\n")
            else:
                f.write("0\n")


def sh(cmd, verbose=True):
    if verbose:
        args = ' '.join(map(lambda s: quote(s), cmd[1:]))
        sys.stderr.write("$ %s %s\n" % (cmd[0], args))
    return subprocess.call(cmd)


def test_path(subdir):
    return os.path.join(TEST_PATH, subdir)


def dataset_home(subdir):
    return os.path.join(DATASET_HOME, subdir)


class Result:
    def __init__(self,
                 wall_time=None,
                 args=None,
                 log_size=None,
                 perf_stats={}):
        self.wall_time = wall_time
        self.args = args
        self.log_size = log_size
        self.perf_stats = perf_stats

    def _read_file_to_dict(self, path):
        data = {}
        with open(path) as stat_file:
            for line in stat_file:
                key, value = line.split(" ", 1)
                data[key] = value.strip()
        return data

    def read_cpuacct_cgroup(self, cpuacct):
        stat_path = os.path.join(cpuacct.mountpoint, "cpuacct.stat")
        stats = self._read_file_to_dict(stat_path)
        self.system_time = stats["system"]
        self.user_time = stats["user"]
        percpu_path = os.path.join(cpuacct.mountpoint, "cpuacct.usage_percpu")
        with open(percpu_path) as percpu:
            self.time_per_cpu = list(map(int, percpu.read().split()))

    def calculate_compressed_logsize(self, log_path):
        lz4 = subprocess.Popen(('lz4c', '--stdout', log_path),
                               stdout=subprocess.PIPE)
        output = subprocess.check_output(('wc', '--bytes'), stdin=lz4.stdout)
        lz4.wait()
        self.compressed_logsize = int(output)

EVENTS = [
         "branch-instructions",
         "bus-cycles",
         "cache-misses",
         "cache-references",
         "cpu-cycles",
         "instructions",
         "ref-cycles",
         "alignment-faults",
         "context-switches",
         "cpu-clock",
         "cpu-migrations",
         "major-faults",
         "minor-faults",
         "page-faults",
         "task-clock"
]


class PerfStat():
    def __init__(self, cgroup_name, perf_command="perf"):
        self.cmd = [perf_command,
                    "stat",
                    "--field-separator", "\t",
                    "--all-cpus",
                    "--event", ",".join(EVENTS),
                    "--cgroup", cgroup_name]
        print(" ".join(self.cmd))

    def run(self):
        self.process = subprocess.Popen(self.cmd,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)

    def result(self):
        try:
            self.process.send_signal(signal.SIGINT)
        except OSError as e:
            print("perf is already stopped: %s" % e)
        stdout, stderr = self.process.communicate()
        stats = {}
        for l in stderr.decode("utf-8").split("\n"):
            columns = l.split("\t")
            if len(columns) < 3:
                continue
            value = columns[0]
            name = columns[2]
            stats[name] = value
        if len(stats) == 0:
            raise OSError("could not obtain statistics from perf: %s" %
                          stderr.decode("utf-8"))
        return stats


class Benchmark():
    def __init__(self, name, args, command=None, env={}):
        self.name = name
        self._args = args
        if command is None:
            self.command = name
        else:
            self.command = command
        self.perf_command = "perf"
        self.env = env

    def args(self, cores=16):
        res = []
        for arg in self._args:
            if issubclass(type(arg), NCores):
                res.append(str(arg.to_param(cores)))
            else:
                res.append(str(arg))
        return res

    def run(self, cores, perf_log, with_pt, with_tthread):
        os.chdir(test_path(self.name))
        cmd = ["./" + self.command] + self.args(cores)
        if with_tthread:
            libtthread = inspector.default_tthread_path()
        else:
            libtthread = None
        for c in cmd:
            assert type(c) is not None
        print("$ " + " ".join(cmd) +
              (" pt" if with_pt else "") +
              (" tthread" if with_tthread else ""))
        if os.path.exists(perf_log):
            os.remove(perf_log)
        cgroup_name = "inspector"

        with cgroups.cpuacct(cgroup_name) as cpuacct, \
                cgroups.perf_event(cgroup_name) as perf_event:
            perf = PerfStat(perf_event.name, perf_command=self.perf_command)
            perf.run()
            proc = inspector.run(cmd,
                                 perf_command=self.perf_command,
                                 processor_trace=with_pt,
                                 tthread_path=libtthread,
                                 perf_log=perf_log,
                                 perf_event_cgroup=perf_event,
                                 additional_cgroups=[cpuacct],
                                 env=self.env)
            status = proc.wait()
            if status.exit_code != 0:
                raise OSError("command: %s\nfailed with: %d" %
                              (" ".join(cmd), status.exit_code))
            perf_stats = perf.result()
            r = Result(wall_time=status.duration,
                       args=self.args(cores),
                       log_size=os.path.getsize(perf_log),
                       perf_stats=perf_stats)
            r.read_cpuacct_cgroup(cpuacct)
            r.calculate_compressed_logsize(perf_log)
        return r

increasing_threads_benchmarks = [
    Benchmark("canneal",
              [CannealThreads(),
               10000,
               2000,
               test_path("canneal/100000.nets"),
               32]),
    Benchmark("blacksholes",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32]),
    Benchmark("dedup",
              ["-c",
               "-p",
               "-t", DedupThreads(),
               "-i", test_path("dedup/FC-6-x86_64-disc1.iso"),
               "-o", "output.dat.ddp"]),
    # Benchmark("ferret",
    #           [test_path("ferret/corel"),
    #            "lsh", test_path("ferret/queries"),
    #            10,
    #            20,
    #            1,
    #            "output.txt"]),
    Benchmark("swaptions",
              ["-ns", 128,
               "-sm", 50000,
               "-nt", NCores()]),
    Benchmark("streamcluster",
              [10,
               20,
               128,
               16384,
               16384,
               1000,
               "none",
               "output.txt",
               NCores()]),
    # Benchmark("vips",
    #           ["im_benchmark",
    #            test_path("vips/orion_18000x18000.v"),
    #            "output.v"]),
    # Benchmark("raytrace",
    #           [test_path("raytrace/thai_statue.obj"),
    #            "-automove",
    #            "-nthreads",
    #            NCores(),
    #            "-frames 200",
    #            "-res 1920 1080"],
    #           command="rtview"),
    Benchmark("histogram", [dataset_home("histogram_datafiles/large.bmp")]),
    Benchmark("linear_regression",
              [dataset_home("linear_regression_datafiles/"
                            "key_file_500MB.txt")]),
    Benchmark("reverse_index", [dataset_home("reverse_index_datafiles")]),
    Benchmark("string_match",
              [dataset_home("string_match_datafiles/key_file_500MB.txt")]),
    Benchmark("word_count",
              [dataset_home("word_count_datafiles/word_100MB.txt")]),
    Benchmark("kmeans", ["-d", 3, "-c", 500, "-p", 50000, "-s", 500]),
    Benchmark("matrix_multiply", [2000, 2000]),
    Benchmark("pca", ["-r", 4000, "-c", 4000, "-s", 100])
]

increasing_worksize_benchmarks = [
    Benchmark("word_count-10mb",
              [dataset_home("word_count_datafiles/word_10MB.txt")]),
    Benchmark("word_count-50mb",
              [dataset_home("word_count_datafiles/word_50MB.txt")]),
    Benchmark("word_count-100mb",
              [dataset_home("word_count_datafiles/word_100MB.txt")]),
    Benchmark("linear_regression-50mb",
              [dataset_home("linear_regression_datafiles/"
                            "key_file_50MB.txt")]),
    Benchmark("linear_regression-100mb",
              [dataset_home("linear_regression_datafiles/"
                            "key_file_100MB.txt")]),
    Benchmark("linear_regression-500mb",
              [dataset_home("linear_regression_datafiles/"
                            "key_file_500MB.txt")]),
    Benchmark("string_match-50mb",
              [dataset_home("string_match_datafiles/key_file_50MB.txt")]),
    Benchmark("string_match-100mb",
              [dataset_home("string_match_datafiles/key_file_100MB.txt")]),
    Benchmark("string_match-500mb",
              [dataset_home("string_match_datafiles/key_file_100MB.txt")]),
    Benchmark("histogram-small",
              [dataset_home("histogram_datafiles/small.bmp")]),
    Benchmark("histogram-med",
              [dataset_home("histogram_datafiles/med.bmp")]),
    Benchmark("histogram-large",
              [dataset_home("histogram_datafiles/large.bmp")]),
]

increasing_computation_benchmarks = [
    Benchmark("swaptions-16",
              ["-ns", 128,
               "-sm", 50000,
               "-nt", NCores()]),
    Benchmark("swaptions-8",
              ["-ns", 128,
               "-sm", 25000.0,
               "-nt", NCores()]),
    Benchmark("swaptions-4",
              ["-ns", 128,
               "-sm", 12500.0,
               "-nt", NCores()]),
    Benchmark("swaptions-2",
              ["-ns", 128,
               "-sm", 6250.0,
               "-nt", NCores()]),
    Benchmark("swaptions-1",
              ["-ns", 128,
               "-sm", 3125.0,
               "-nt", NCores()]),
    Benchmark("blacksholes-1",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32],
              env={"NUM_RUNS": 6}),
    Benchmark("blacksholes-2",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32],
              env={"NUM_RUNS": 12}),
    Benchmark("blacksholes-4",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32],
              env={"NUM_RUNS": 25}),
    Benchmark("blacksholes-8",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32],
              env={"NUM_RUNS": 50}),
    Benchmark("blacksholes-16",
              [8,
               test_path("blacksholes/in_10M.txt"),
               test_path("canneal/prices.txt"),
               32],
              env={"NUM_RUNS": 100}),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Run benchmarks.")
    parser.add_argument("--perf-command",
                        default="perf",
                        help="Path to perf tool")
    parser.add_argument("--perf-log",
                        default="perf.data",
                        help="Path to perf log")
    parser.add_argument("output",
                        default=".",
                        help="output directory to write measurements")
    return parser.parse_args()


def build_project():
    sh(["cmake", "-DCMAKE_BUILD_TYPE=Release", "-DBENCHMARK=On"])
    sh(["cmake", "--build", "."])
    sh(["cmake", "--build", ".", "--target", "build-parsec"])
    sh(["cmake", "--build", ".", "--target", "build-phoenix"])


def main():
    args = parse_args()
    output = os.path.realpath(args.output)
    perf_log = os.path.realpath(args.perf_log)

    if "/" in args.perf_command:
        # resolve relatives command paths
        perf_command = os.path.realpath(args.perf_command)
    else:
        perf_command = args.perf_command

    os.chdir(os.path.join(SCRIPT_ROOT, "../.."))

    path = os.path.join(output, "log.json")

    build_project()

    if os.path.exists(path):
        log = json.load(open(path))
    else:
        log = {}

    for threads in [16, 8, 4, 2]:
        os.environ["IM_CONCURRENCY"] = str(threads)
        set_online_cpus(threads)
        for bench in increasing_threads_benchmarks:
            run_name = "%s-%d" % (bench.name, threads)
            bench.perf_command = perf_command
            try:
                sys.stderr.write(">> run %s\n" % bench.name)

                if run_name not in log:
                    log[run_name] = {
                            "threads": threads,
                            "libs": {},
                            "args": [],
                    }

                def run(name, pt, tthread):
                    libs = log[run_name]["libs"]
                    if name not in libs:
                        libs[name] = {
                                "times": [],
                                "log_sizes": [],
                                "compressed_logsizes": [],
                                "system_time": [],
                                "user_time": [],
                                "time_per_cpu": [],
                                "args": None
                        }
                        for event in EVENTS:
                            libs[name][event] = []
                    runs = max(6 - len(libs[name]["times"]), 0)
                    if runs <= 0:
                        print("skip %s -> %d" % (name, runs))
                    for i in range(runs):
                        result = bench.run(threads,
                                           perf_log,
                                           pt,
                                           tthread)
                        lib = libs[name]
                        lib["times"].append(result.wall_time)
                        lib["log_sizes"].append(result.log_size)
                        lib["compressed_logsizes"].append(result.compressed_logsize)
                        lib["system_time"].append(result.system_time)
                        lib["user_time"].append(result.user_time)
                        lib["time_per_cpu"].append(result.time_per_cpu)
                        for event in EVENTS:
                            lib[event].append(result.perf_stats[event])
                        log[run_name]["args"] = result.args
                        with open(path, "w") as f:
                            json.dump(log, f, sort_keys=True, indent=4)

                run("pthread",   False, False)
                run("tthread",   False, True)
                run("pt",        True,  False)
                run("inspector", True,  True)

            except OSError as e:
                print("failed to run %s: %s" % (bench.name, e))

if __name__ == '__main__':
    main()
