import sys
import json
from collections import defaultdict
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


FIELDS = [
    "times",
    "log_sizes",
    "user_time",
    "alignment-faults",
    'branch-instructions',
    'bus-cycles',
    'cache-misses',
    'cache-references',
    'cpu-cycles',
    'instructions',
    'ref-cycles',
    'context-switches',
    'cpu-clock',
    'cpu-migrations',
    'major-faults',
    'minor-faults',
    'page-faults',
    'task-clock'
]

alias_map = {
        "pthread": "pthread",
        "pt": "os support",
        "tthread": "threading library",
        "inspector": "xy"
}

bench_alias_map = {
        "linear_regression": "linear_reg",
        "matrix_multiply": "matrix_mult",
        "reverse_index": "reverve_idx",
        "streamcluster": "streamclust."
}


def to_float(v):
    try:
        if isinstance(v, str):
            return float(v.replace(",", "."))
        else:
            return float(v)
    except:
        return 0


def deserialize(json_file):
    measurements = defaultdict(list)
    json_data = json.load(json_file)
    for benchmark, data in json_data.items():
        name, _ = benchmark.split("-", 1)
        for lib, lib_data in data["libs"].items():
            field_count = 0
            for field in FIELDS:
                try:
                    field_count = max(len(lib_data[field]), field_count)
                    for v in lib_data[field]:
                        measurements[field].append(to_float(v))
                except KeyError:
                    measurements[field].append(0)
            for i in range(field_count):
                measurements["name"].append(bench_alias_map.get(name, name))
                measurements["library"].append(alias_map[lib])
                for key in ["args", "threads", "variant", "size"]:
                    try:
                        if data[key] is None:
                            measurements[key].append("")
                        else:
                            measurements[key].append(data[key])
                    except KeyError:
                        measurements[key].append("")
    for k, v in measurements.items():
        print("%s: %d" % (k, len(v)))
    return pd.DataFrame(measurements)


def tmean(df):
    return (df.sum() - df.min() - df.max()) / (df.count() - 2)


def relative_to_pthread(df):
    grouped = df.groupby(['library', 'name', 'threads', 'variant'])
    tmean_values = tmean(grouped).reset_index()
    wrt_native = None
    for lib in df['library'].unique():
        if lib == 'pthread':
            continue
        by_lib = tmean_values[tmean_values.library == lib]
        native = tmean_values[tmean_values.library == 'pthread']
        merged = by_lib.merge(native, on=['name', 'threads', 'variant'])
        data = {}
        for field in FIELDS:
            data[field] = merged[field + "_x"] / merged[field + "_y"]
        data['library'] = merged["library_x"]
        data['size'] = merged["size_x"]
        for field in ['name', "threads", 'variant']:
            data[field] = merged[field]

        if wrt_native is None:
            wrt_native = pd.DataFrame(data)
        else:
            wrt_native = wrt_native.append(pd.DataFrame(data))
    return wrt_native


class Graph:
    def __init__(self, df, format):
        self.df = df
        self.format = format

    def save(self, graph, name):
        file_name = ("%s.%s" % (name, self.format)).replace(" ", "_")
        print(file_name)
        graph.savefig(file_name)

    def setup(self, graph, annotations, xticks):
        for annotation in annotations:
            graph.fig.text(annotation)
        graph.set_xlabels("")
        graph.despine(left=True)
        if len(xticks) > 5:
            graph.set_xticklabels(xticks, rotation=50)
        else:
            graph.set_xticklabels(xticks)

    def by_variant(self, y, annotations=[], ylim=(0, 10)):
        for lib in self.df['library'].unique():
            if lib == 'pthread':
                continue
            filter = (self.df.library == lib) & \
                     (self.df.threads == 16)
            by_lib = self.df.copy()[filter]

            f, ax1 = plt.subplots()
            g = sns.barplot(x="name",
                            y=y,
                            hue="variant",
                            data=by_lib,
                            palette="Greys",
                            hue_order=["small", "medium", "large"],
                            ax=ax1)
            g.set_ylabel("Overhead w.r.t native execution")
            g.set_ylim(ylim)
            g.set_xticklabels(by_lib.name.unique())
            g.set_xlabel("")

            sorted_ = by_lib.sort_values(['name', 'variant'], ascending=[1, 0])
            ax2 = ax1.twinx()
            ax2.set_ylim((0, 1500))
            ax2.grid(False)
            sizes = sorted_["size"]

            for offset, name in enumerate(sorted_["name"].unique()):
                index = (np.arange(3) - 1) / 3
                ax2.plot(index + offset,
                         sizes[offset * 3:(offset + 1) * 3],
                         color="k",
                         marker='o')
                ax2.set_ylabel("Input size [MB]")

            self.save(plt, "worksize-%s-%s" % (y, lib))

    def by_library(self, y, annotations=[]):
        for lib in self.df['library'].unique():
            if lib == 'pthread':
                continue
            by_lib = self.df.copy()[self.df.library == lib]

            g = sns.factorplot(x="name",
                               y=y,
                               hue="threads",
                               data=by_lib,
                               kind="bar",
                               palette="Greys",
                               legend_out=False,
                               aspect=2)
            g.set_ylabels("Overhead w.r.t native execution")
            g.set(ylim=(0, 10))
            self.setup(g, annotations, by_lib.name.unique())
            self.save(g, "%s-%s" % (y, lib))

    def by_threads(self, y, annotations=[]):
        for thread in self.df['threads'].unique():
            filter = (self.df.threads == thread) & \
                    (self.df.library != 'pthread')
            by_thread = self.df[filter]

            g = sns.factorplot(x="name",
                               y=y,
                               hue="library",
                               data=by_thread,
                               kind="bar",
                               palette="Greys",
                               legend_out=False,
                               aspect=2)
            g.set_ylabels("Overhead w.r.t native")
            g.set(ylim=(0, 10))
            self.setup(g, annotations, by_thread.name.unique())
            self.save(g, "%s-%d-threads" % (y, thread))


def main(action, json_path):
    df = deserialize(open(json_path))
    wrt_native = relative_to_pthread(df)

    sns.set_context("poster", font_scale=1.5)
    sns.set(style="whitegrid")
    g = Graph(wrt_native, "pdf")

    if action == "worksize":
        for f in ["times", "cpu-cycles"]:
            g.by_variant(f, ylim=(0, 4))
    else:
        for f in FIELDS:
            g.by_library(f)
            g.by_threads(f)


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def usage():
    die("USAGE: %s threads|worksize JSON" % sys.argv[0])

if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()
    action = sys.argv[1]
    json_path = sys.argv[2]
    main(action, json_path)
