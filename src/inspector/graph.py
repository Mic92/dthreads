import sys
import json
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from collections import defaultdict, OrderedDict


def to_alphanum(key):
    def convert(text):
        return int(text) if text.isdigit() else text

    def split(s):
        return [convert(c) for c in re.split('([0-9]+)', key(s))]
    return split


def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2.,
                 height * 0.5,
                 '%.2f' % height,
                 ha='center',
                 va='bottom',
                 rotation="vertical")

PATTERNS = (' ', '////', 'x', ' ', '*', 'o', 'O', '.')


def mean(v):
    v_ = list(map(float, v))
    if len(v_) < 4:
        return np.std(v_)
    v_.remove(max(v_))
    v_.remove(min(v_))
    return np.mean(v_)


def std(v):
    v_ = list(map(float, v))
    if len(v_) < 4:
        return np.std(v_)
    v_.remove(max(v_))
    v_.remove(min(v_))
    return np.std(v_)


alias_map = {
        "pthread": "pthread",
        "pt": "pt",
        "tthread": "xy library",
        "inspector": "xy"
}


def generate_graph1(log):
    def constructor():
        return defaultdict(OrderedDict)
    per_thread = defaultdict(constructor)
    for bench, per_lib in sorted(json.load(open(log)).items()):
        name, threads = bench.split("-", 1)
        for lib, data in per_lib["libs"].items():
            per_thread[int(threads)][lib][bench] = data

    bar_width = 0.40
    opacity = 0.4

    plt.figure(figsize=(10, 8))

    for thread, benchmarks in per_thread.items():
        bench_names = benchmarks["pthread"].keys()
        bench_names = map(lambda n: n.split("-", 1)[0], bench_names)
        pthread_values = []
        for v in benchmarks["pthread"].values():
            pthread_values.append(mean(v["times"]))

        i = 0
        for lib, per_lib in benchmarks.items():

            normalized_values = []
            std_values = []
            for v, w in zip(per_lib.values(), pthread_values):
                normalized_values.append(mean(v["times"]) / w)
                std_values.append(std(map(lambda v: float(v)/w, v["times"])))
            index = np.arange(0, len(normalized_values) * 2, 2)
            plt.bar(index + bar_width * (i + 0.5),
                    normalized_values,
                    bar_width,
                    yerr=std_values,
                    alpha=opacity,
                    label=alias_map[lib],
                    hatch=PATTERNS[i],
                    color=cm.Greys(1.*i/len(benchmarks)),
                    error_kw=dict(ecolor='black'))
            # autolabel(rect)
            i += 1
            index += 0
        plt.xlabel('Benchmarks')
        plt.ylabel('Overhead')
        plt.title("Times by benchmarks and libraries for %d threads" %
                  thread,
                  y=1.00)
        plt.xticks(index + bar_width,
                   [n for n in bench_names],
                   rotation=50)
        plt.legend(loc='best')
        plt.grid()

        plt.tight_layout()
        plt.savefig("benchmarks-%d.pdf" % thread)
        plt.clf()


def generate_graph2(log, aspect, title):
    def constructor():
        return defaultdict(OrderedDict)
    per_lib = defaultdict(constructor)
    json_data = json.load(open(log))
    bench_names = set()
    sort = sorted(json_data.items(),
                  key=to_alphanum(lambda pair: pair[0]),
                  reverse=False)
    for bench, per_bench in sort:
        name, threads = bench.split("-", 1)
        bench_names.add(name)
        for lib, data in per_bench["libs"].items():
            per_lib[lib][int(threads)][bench] = data

    bar_width = 0.40
    opacity = 0.4

    pthread = per_lib["pthread"]

    for lib, per_thread in per_lib.items():
        i = 0
        for thread, data in sorted(per_thread.items()):
            pthread_values = []
            for v in pthread[thread].values():
                pthread_values.append(mean(v[aspect]))

            normalized_values = []
            std_values = []
            for v, w in zip(data.values(), pthread_values):
                normalized_values.append(mean(v[aspect]) / w)
                std_values.append(std(map(lambda v: float(v)/w, v[aspect])))
            index = np.arange(0, len(normalized_values) * 2, 2)
            plt.bar(index + bar_width * (i + 0.5),
                    normalized_values,
                    bar_width,
                    yerr=std_values,
                    alpha=opacity,
                    label="%d threads" % thread,
                    hatch=PATTERNS[i],
                    color=cm.Greys(1.*i/len(per_lib)),
                    error_kw=dict(ecolor='black'))
            # autolabel(rect)
            i += 1
            index += 0
        plt.xlabel('Benchmarks')
        plt.ylabel('Overhead')
        plt.title(title % alias_map[lib],
                  y=1)
        plt.xticks(index + bar_width * 2,
                   sorted(bench_names),
                   rotation=60)
        plt.legend(loc='best')
        plt.grid()

        plt.savefig("benchmarks-%s-%s.pdf" % (aspect, lib))
        plt.tight_layout()
        plt.clf()


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        die("USAGE: %s log.json" % sys.argv[0])
    generate_graph1(sys.argv[1])
    generate_graph2(sys.argv[1],
                    "times",
                    "Times by benchmarks and threads for %s")
    generate_graph2(sys.argv[1],
                    "cpu-cycles",
                    "CPU cycles by benchmarks and threads for %s")

if __name__ == "__main__":
    main()
