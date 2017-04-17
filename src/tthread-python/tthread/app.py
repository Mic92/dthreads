#!/usr/bin/env python

import sys
import os
import argparse
import tthread

from tthread import formats


def abort(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)

formats = {
    "dtl": formats.DTLWriter,
    "tsv": formats.TsvWriter,
    "tsv2": formats.Tsv2Writer,
}

supported_formats = ", ".join(formats.keys())


def parse_arguments():
    desc = "Run program with libtthread."
    parser = argparse.ArgumentParser(description=desc)
    h1 = "path to libtthread.so " \
         "(default: ../../libtthread.so - relative to script path)"
    parser.add_argument("--libtthread-path",
                        nargs="?",
                        default=tthread.default_library_path(),
                        help=h1)
    h2 = "path to trace file (default: stdout); " \
         "if no trace file is specified, " \
         "stdout of programm is redirected to stderr"
    parser.add_argument("--output", nargs="?",
                        help=h2)
    h3 = "default format to write access log " \
         "(supported: %s; default: dtl)" % supported_formats
    parser.add_argument("--format", nargs="?",
                        default="dtl",
                        help=h3)
    parser.add_argument("command", nargs=1,
                        help="command to execute with")
    parser.add_argument("arguments", nargs="*",
                        default=[],
                        help="arguments passed to command")
    return parser.parse_args()


def main():
    if sys.version_info < (3, 0):
        abort("this script requires Python 3.x, not Python 2.x")
    args = parse_arguments()

    if not os.path.isfile(args.libtthread_path):
        abort("Libthreads not found at '%s', "
              "use --libtthread-path to specify it" % args.libtthread_path)

    formatter = formats.get(args.format, None)
    if formatter is None:
        abort("unsupported format %s, supported formats are %s" %
              (args.format, supported_formats))

    if args.output is None:
        stdout = sys.stderr
        output = sys.stdout
    else:
        stdout = sys.stdout
        try:
            output = open(args.output)
        except EnvironmentError as e:
            abort("failed to open output '%s'" % e)

    command = args.command + args.arguments
    try:
        process = tthread.run(command, args.libtthread_path, stdout=stdout)
        log = process.wait()
        if log.return_code != 0:
            print("process exited with: %d" % log.return_code, file=sys.stderr)
    except tthread.Error as e:
        abort("Execution fails: %s" % e)
    formatter(log).write(output)
