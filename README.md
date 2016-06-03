Inspector
---------

Builds Data Provenance Graph for Multi-Threaded Programms
using [tthread](src/tthread) and
[Intel Pt](https://software.intel.com/en-us/blogs/2013/09/18/processor-tracing).

For further information can be found [here](src/inspector)

[Paper](http://wp.mpi-sws.org/bhatotia/files/2016/04/PID4226263.pdf) on Inspector published at ICDCS 2016

## TThread ##

- TThread is a Tracing Multithreading library
- TThread is based on [dthreads](https://github.com/emeryberger/dthreads)
- For python api and scripts, see [here](src/tthread-python/README.md)


### Building Tthread ###

[CMake](http://www.cmake.org/) is required to build Tthread:

```
$ git clone https://github.com/Mic92/tthread.git
$ git submodule update --init
$ cd tthread
$ cmake .
$ make
```

This will build the Tthread library (`libtthread.so`).

To build tthread with debugging options use:

```
cmake -DCMAKE_BUILD_TYPE=DEBUG .
```

### Using Tthread ###

Tthread currently only supports Linux/x86\_64 platforms.

1. Compile your program to object files (here, we use just one, `target.o`).

2. Link to the tthread library. There are three options (neither of which
   is particular to tthread).

  (a) Dynamic linking: this approach requires no environment variables,
      but the tthread library needs to be in a fixed, known location.
      Place the tthread library in a directory (`TTHREAD_DIR`).
      Then compile your program as follows:

```
% g++ target.o -rdynamic <TTHREAD_DIR>/libtthread.so -ldl -o target
```

  (b) Ordinary dynamic linking: this approach is more flexible (you can
      change the location of the tthread library), but you must also
      set the `LD_LIBRARY_PATH` environment variable.

```
% g++ target.o -L<TTHREAD_DIR> -ltthread -dl -o target
% export LD_LIBRARY_PATH=<TTHREAD_DIR>:$LD_LIBRARY_PATH
```

  (c) set LD_PRELOAD to <TTHREAD_DIR>/libtthread.so (Compile program with -rdynamic is also needed in this case)

```
export LD_PRELOAD=<TTHREAD_DIR>/libtthread.so
```

To read the access log at runtime, take a look at [Usage.md](Usage.md)

### Run the Tests ###

```
cmake -DTESTING=ON .
make test
```
