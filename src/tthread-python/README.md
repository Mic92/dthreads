# Python3 library to process output of libtthread

Example usage:

For library usage see `example.py`

```python
import tthread
path = "<path/to/libtthread.so>"
binary = "<path/to/binary>"
process = tthread.run(binary, path)
log = process.wait()
for event in log.read():
    print(event)
```

Supported events:

- InvalidEvent
- WriteEvent
- ReadEvent
- ThunkEvent
- FinishEvent

All these events contain the following fields:

- **type** how memory was access (read/write)
- **return\_address** return address, which issued the first page fault on this page
- **thread\_id** process id, which accessed the memory

ThunkEvent contain the following additional field:

- **id** when ever a synchronisation point (lock/mutex) a new thunk starts in a
    thread and gets a number assigned

Read-/WriteEvent contain the following additional field:

- **address** memory address which was accessed

FinishEvent before a thread exists

To check in which memory space an address belongs to use

```python
log.is_heap(<addr>)
log.is_global(<addr>)
log.is_mmap(<addr>)
```

To generate tab-seperated log files use `tthread` application in `bin`:

```bash
$ ./bin/tthread --help
usage: tthread [-h] [--libtthread-path [LIBTTHREAD_PATH]] [--output [OUTPUT]]
               [--format [FORMAT]]
               command [arguments [arguments ...]]

Process some integers.

positional arguments:
  command               command to execute with
  arguments             arguments passed to command

optional arguments:
  -h, --help            show this help message and exit
  --libtthread-path [LIBTTHREAD_PATH]
                        path to libtthread.so (default: ../../libtthread.so -
                        relative to script path)
  --output [OUTPUT]     path to trace file (default: stdout); if no trace file
                        is specified, stdout of programm is redirected to
                        stderr
  --format [FORMAT]     default format to write access log (supported: tsv,
                        tsv2; default: tsv)
```

In the following example the `matrix_mutiply` benchmark from Phoenix is run.

```bash
$ ./bin/tthread --format=tsv -- ../../eval/tests/matrix_multiply/matrix_multiply-tthread 2000 2000 > matrixmultiply.tsv
```

The log output is written to `matrixmultiply.tsv`

```bash
$ head -n 10
type    return_address  thread_id   id/address  heap/global
write   140049782290516 8768    140041133006848 heap
write   140049782290516 8768    140041149788160 heap
thunk   94525342585784  8768    1   -
thunk   94525342585784  8768    2   -
write   140049782269278 8768    140041149788208 heap
thunk   94525342585784  8768    3   -
write   140049782269278 8768    140041149788216 heap
thunk   94525342585784  8768    4   -
write   140049782269278 8768    140041149788224 heap
```

As it is tab-seperated in can be imported into spreadsheet application without any additional tools.

# Disable ASLR

For some benchmarks it is important to disable address space layout
randomization. One way to do this is to start a new shell as:

```bash
setarch $(uname -m) -R $SHELL
```

Or prefix logger script:

```bash
$ setarch $(uname -m) -R ./bin/tthread .
```
