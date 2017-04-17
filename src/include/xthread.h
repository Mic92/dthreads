#pragma once

#include <errno.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "real.h"
#include "xdefines.h"

// Heap Layers
#include "heaplayers/freelistheap.h"

extern "C" {
// The type of a pthread function.
typedef void *threadFunction (void *);
}

class xrun;

class xthread {
private:

  /// @class ThreadStatus
  /// @brief Holds the thread id and the return value.
  class ThreadStatus {
public:

    ThreadStatus(void *r, bool f) : retval(r), forked(f) {}

    ThreadStatus(void) {}

    volatile int tid;

    int threadIndex;
    void *retval;

    bool forked;
  };

  /// Current nesting level (i.e., how deep we are in recursive threads).
  unsigned int _nestingLevel;

  /// Thread's PID
  int _tid;

  xrun& _run;

public:

  xthread(xrun& run) :
    _nestingLevel(0),
    _run(run)
  {}

  void *spawn(const void     *caller,
              threadFunction *fn,
              void           *arg,
              int            threadindex);

  void join(void *v,
            void **result);

  int cancel(void *v);
  int thread_kill(void *v,
                  int  sig);

  // The following functions are trying to get id or thread index of specified
  // thread.
  int getThreadIndex();
  int getThreadIndex(void *v);
  int getThreadPid(void *v);

  // represent sub-computation between synchronisation points
  int _thunkId;

  // getId is trying to get id for current thread, the function
  // is called by current thread.
  inline int getId(void) {
    return _tid;
  }

  inline int getThunkId(void) {
    return _thunkId;
  }

  inline void setId(int id) {
    if (id != _tid) {
      // reset thunkId for every new thread
      _thunkId = 0;
    }
    _tid = id;
  }

  inline void startThunk() {
    _thunkId++;
  }

private:

  void *forkSpawn(const void     *caller,
                  threadFunction *fn,
                  ThreadStatus   *t,
                  void           *arg,
                  int            threadindex);

  void run_thread(const void     *caller,
                  threadFunction *fn,
                  ThreadStatus   *t,
                  void           *arg);

  /// @return a chunk of memory shared across processes.
  void *allocateSharedObject(size_t sz) {
    return WRAP(mmap)(NULL,
                      sz,
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_ANONYMOUS,
                      -1,
                      0);
  }

  void freeSharedObject(void *ptr, size_t sz) {
    munmap(ptr, sz);
  }
};
