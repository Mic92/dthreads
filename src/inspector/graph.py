import sys
import json
from collections import defaultdict
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec, ticker
import matplotlib


FIELDS = [
    "times",
    # "log_sizes",
    # "user_time",
    # "alignment-faults",
    # 'branch-instructions',
    # 'bus-cycles',
    # 'cache-misses',
    # 'cache-references',
    'cpu-cycles',
    # 'instructions',
    # 'ref-cycles',
    # 'context-switches',
    # 'cpu-clock',
    # 'cpu-migrations',
    # 'major-faults',
    # 'minor-faults',
    # 'page-faults',
    # 'task-clock'
]

ANNOTATIONS = [
        ("cpu-cycles-Total overheads", 7.9, 38, "620x"),
        ("cpu-cycles-Total overheads", 0.85, 38, "63x"),
]

Y_RANGES = {
        "times-Total overheads": ((0, 8), (8, 37)),
        "times-16-threads": ((0, 8), (8, 30)),
        "cpu-cycles-Total overheads": ((0, 7), (7, 37)),
        "cpu-cycles-16-threads": ((0, 12), (12, 70))
}

alias_map = {
        "pthread": "pthread",
        "pt":        "OS support",
        "tthread":   "Threading lib.",
        "inspector": "Total overheads"
}

bench_alias_map = {
        "linear_regression": "linear_reg",
        "matrix_multiply": "matrix_mul",
        "reverse_index": "reverve_idx",
        "streamcluster": "streamcl.",
        "string_match": "string_ma.",
        "word_count": "word_c",
        "blackscholes": "blackscho."
}


def to_float(v):
    try:
        if isinstance(v, str):
            return float(v.replace(",", "."))
        else:
            return float(v)
    except:
        return 0


def cm2inch(value):
    return value/2.54


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
    def transform(x):
        if x.dtype == list:
            return (x.max(),)
        return (x.sum() - x.min() - x.max()) / (x.count() - 2)
    return df.aggregate(transform)


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

    def add_break_labels(self, ax1, ax2, limit_low, limit_high):
        """
        Code to add diagonal slashes to truncated y-axes. copied from
        http://matplotlib.org/examples/pylab_examples/broken_axis.html
        """
        # how big to make the diagonal lines in axes coordinates
        d = .017
        d1 = d
        d2 = d
        # arguments to pass plot, just so we don't keep repeating them
        kwargs = dict(transform=ax1.transAxes,
                      color='#cccccc',
                      clip_on=False,
                      linewidth=1.0)

        # top-left diagonal
        ax1.plot((-d1, +d1), (-d1 - 0.025, +d1 + 0.025), **kwargs)
        # top-right diagonal
        ax1.plot((1 - d1, 1 + d1), (-d1 - 0.025, +d1 + 0.025), **kwargs)

        # switch to the bottom axes
        kwargs.update(transform=ax2.transAxes)
        # bottom-left diagonal
        ax2.plot((-d2, +d2), (1 - d2, 1 + d2), **kwargs)
        # bottom-right diagonal
        ax2.plot((1 - d2, 1 + d2), (1 - d2, 1 + d2), **kwargs)

    def discontinue(self, ax1, ax2, name):
        if name in Y_RANGES:
            limit_low = Y_RANGES[name][0]
            limit_high = Y_RANGES[name][1]
        else:
            limit_low = (0, 60)
            limit_high = (60, 70)

        ax1.set_ylim(limit_high)
        ax1.spines['bottom'].set_visible(False)
        ax1.xaxis.set_visible(False)
        ax1.yaxis.set_major_locator(ticker.MultipleLocator(10))

        ax2.tick_params(labeltop='off')
        ax2.set_ylim(limit_low)
        ax2.spines['top'].set_visible(False)
        ax2.xaxis.tick_bottom()

        self.add_break_labels(ax1, ax2, limit_low, limit_high)

    def annotate(self, name, ax=plt):
        for annotation in ANNOTATIONS:
            name_, x, y, text = annotation
            if name_ == name:
                ax.text(x, y, text)

    def show(self, name):
        ydata = []

        def on_click(event):
            if event.inaxes is None:
                return
            if event.button == 3:
                ydata.append("%.1fx" % event.ydata)
                return
            if event.button != 1:
                return
            import pymsgbox
            text = pymsgbox.prompt(default="-".join(ydata),
                                   title='Label for Text')
            print('("%s", %f, %f, "%s"),' %
                  (name, event.xdata, event.ydata, text),
                  file=sys.stderr)
        plt.connect('button_press_event', on_click)
        plt.show()

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
                            hue_order=["small", "medium", "large"],
                            ax=ax1)
            g.set_ylabel("Overhead w.r.t native execution")
            g.set_xticklabels(by_lib.name.unique())
            g.set_xlabel("")
            self.upcase_graphs(ax1, "Variant")

            sorted_ = by_lib.sort_values(['name', 'variant'], ascending=[1, 0])
            ax2 = ax1.twinx()
            ax2.grid(False)
            sizes = sorted_["size"]

            for offset, name in enumerate(sorted_["name"].unique()):
                index = (np.arange(3) - 1) / 3
                ax2.plot(index + offset,
                         sizes[offset * 3:(offset + 1) * 3],
                         color="k",
                         marker='o')
                ax2.set_ylabel("Input size [MB]")

            name = "worksize-%s-%s" % (y, lib)
            g.set_ylim(ylim)
            ax1.set_ylim((0, 7))
            ax2.set_ylim((0, 1500))
            self.annotate(name, ax=ax1)
            xmin, xmax = ax1.get_xlim()
            ax1.set_xlim((xmin - 0.1, xmax + 0.1))
            self.save(plt, name)

    def upcase_graphs(self, ax, title):
        ax.legend(loc='best', title=title)

    def by_library(self, y, annotations=[]):
        for lib in self.df['library'].unique():
            if lib == 'pthread':
                continue
            by_lib = self.df.copy()[self.df.library == lib]

            gs = gridspec.GridSpec(2, 1, height_ratios=[1, 3])

            ax1 = plt.subplot(gs[0])
            ax2 = plt.subplot(gs[1])
            for ax in [ax1, ax2]:
                g = sns.barplot(x="name",
                                y=y,
                                hue="threads",
                                data=by_lib,
                                hue_order=[2, 4, 8, 16, 14, 15],
                                ax=ax)
                g.set_xlabel("")
                self.upcase_graphs(ax, "Threads")
            ax1.legend().set_visible(False)
            xticks = by_lib.name.unique()
            if len(xticks) > 5:
                ax2.set_xticklabels(xticks, rotation=65)

            ax2.set_ylabel("Overhead w.r.t native execution")
            ax1.set_ylabel("")
            name = "%s-%s" % (y, lib)
            self.discontinue(ax1, ax2, name)
            plt.subplots_adjust(hspace=0.2)
            plt.tight_layout(h_pad=1, w_pad=10)
            self.annotate(name, ax=ax1)
            self.save(plt, name)

    def by_threads(self, y, annotations=[]):
        df = self.df
        for thread in df['threads'].unique():
            filter = df.library != 'pthread'

            if thread == 16:
                filter = filter & \
                         (((df.threads == 16) & (df.name != "streamcl.")) | \
                          (df.threads == 15) & (df.name == "streamcl."))
            else:
                filter = filter & (df.threads == thread)

            by_thread = df[filter]

            gs = gridspec.GridSpec(2, 1, height_ratios=[1, 3])

            ax1 = plt.subplot(gs[0])
            ax2 = plt.subplot(gs[1])

            for ax in [ax1, ax2]:
                g = sns.barplot(x="name",
                                y=y,
                                hue="library",
                                data=by_thread,
                                hue_order=["Total overheads",
                                           "Threading lib.",
                                           "OS support"],
                                ax=ax)
                g.set_xlabel("")
                self.upcase_graphs(ax, "Library")
            ax1.set_ylabel("")
            ax1.legend().set_visible(False)

            xticks = by_thread.name.unique()
            if len(xticks) > 5:
                ax2.set_xticklabels(xticks, rotation=65)

            ax2.set_ylabel("Overhead w.r.t native execution")
            ax1.set_ylabel("")
            name = "%s-%d-threads" % (y, thread)
            self.discontinue(ax1, ax2, name)
            plt.subplots_adjust(hspace=0.2)
            plt.tight_layout(h_pad=1, w_pad=10)
            self.annotate(name, ax=ax1)
            self.save(plt, name)


def main(action, json_path):
    matplotlib.rcParams.update({'font.size': 22})
    df = deserialize(open(json_path))
    wrt_native = relative_to_pthread(df)

    sns.set(style="whitegrid")
    sns.set_context("poster", font_scale=1.3)
    g = Graph(wrt_native, "png")

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
