#pragma once

/*
   Author: Emery Berger, http://www.cs.umass.edu/~emery

   Copyright (c) 2007-8 Emery Berger, University of Massachusetts Amherst.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

 */

/*
 * @file   xrun.h
 * @brief  The main engine for consistency management, etc.
 * @author Emery Berger <http://www.cs.umass.edu/~emery>
 * @author Tongping Liu <http://www.cs.umass.edu/~tonyliu>
 * @author Charlie Curtsinger <http://www.cs.umass.edu/~charlie>
 */

#include <link.h>
#include <pthread.h>

// Common defines
#include "xdefines.h"

// threads
#include "xthread.h"

// memory
#include "xmemory.h"

#include "determ.h"

#include "xlogger.h"

#include "xtime.h"

#include "xsched.h"

#include "objectheader.h"

class xrun {
private:

  volatile bool _isCopyOnWrite;
  pid_t _master_thread_id;
  size_t _thread_index;
  bool _fence_enabled;
  size_t _children_threads_count;
  size_t _lock_count;
  bool _token_holding;
  xmemory& _memory;
  xlogger& _logger;
  xsched& _sched;
  xthread _thread;
  xtime _cpu_time;
  determ& _determ;

public:

  /// @brief Initialize the system.
  xrun(xmemory& memory,
       xlogger& logger,
       xsched& xsched) :
    _isCopyOnWrite(false),
    _thread_index(0),
    _fence_enabled(false),
    _children_threads_count(0),
    _lock_count(0),
    _token_holding(false),
    _memory(memory),
    _logger(logger),
    _sched(xsched),
    _thread(*this),
    _determ(determ::newInstance(memory))
  {
    DEBUG("initializing xrun");

    pid_t pid = syscall(SYS_getpid);
    _master_thread_id = pid;

    _thread.setId(pid);

    logger.setThread(&_thread);

    _sched.setThread(&_thread);

    // xmemory.initialize should happen before others
    _memory.initialize(logger);
    _memory.setThreadIndex(0);

    _determ.initialize();
    xbitmap::getInstance().initialize();

    // Add myself to the token queue.
    _determ.registerMaster(_thread_index, pid);
  }

  // Control whether we will copy on writed memory or not.
  // When there is only one thread in the system, memory is not
  // copy on write to avoid the overhead of merging pages back.
  void setCopyOnWrite(bool copyOnWrite) {
    if (copyOnWrite == _isCopyOnWrite) {
      return;
    }
    _isCopyOnWrite = copyOnWrite;

    _memory.setCopyOnWrite(copyOnWrite);
  }

  void finalize(void) {
    _memory.finalize();
  }

  // @ Return the main thread's id.
  inline bool isMaster(void) {
    return getpid() == _master_thread_id;
  }

  // @return the "thread" id.
  inline int id(void) {
    return _thread.getId();
  }

  // New created thread should call this.
  // Now only the current thread is active.
  int childRegister(int pid,
                    int parentindex) {
    // Get the global thread index for this thread, which will be used
    // internally.
    _thread_index = xatomic::increment_and_return(&global_data->thread_index);
    _lock_count = 0;
    _token_holding = false;

    // For child, fence is always enabled in the beginning.
    // Wait on token to do synchronizations if we set this.
    _fence_enabled = true;

    _determ.registerThread(_thread_index, pid, parentindex);

    // Set correponding heap index.
    _memory.setThreadIndex(_thread_index);

    // New thread will not own any blocks in the beginning
    // We should cleanup all blocks information inherited from the parent.
    _memory.cleanupOwnedBlocks();

    _cpu_time.init();
    return _thread_index;
  }

  // New created threads are waiting until notify by main thread.
  void waitParentNotify(void) {
    _determ.waitParentNotify();
  }

  inline void waitChildRegistered(void) {
    _determ.waitChildRegistered();
  }

  void threadDeregister(const void *caller) {
    waitToken();

    _memory.finalcommit(false);

    DEBUGF("%d: thread %lu deregister, get token\n", getpid(), _thread_index);
    atomicEnd();

    // Remove current thread and decrease the fence
    _determ.deregisterThread(_thread_index);

    // tthread::EventData d;
    // d.end.cpu_time = _cpu_time.get();
    // _logger.add(tthread::logevent(tthread::logevent::END,
    //                               caller,
    //                               d));

    _logger.add(tthread::logevent(tthread::logevent::FINISH,
                                  caller,
                                  tthread::EventData()));
  }

  inline void closeFence(void) {
    _fence_enabled = false;
    _children_threads_count = 0;

    // Reclaiming the thread index, new threads can share the same heap with
    // previous exiting threads. Thus we could improve the locality.
    global_data->thread_index = 1;
  }

  void forceThreadCommit(void *v) {
    int pid;

    pid = _thread.getThreadPid(v);
    _memory.forceCommit(pid);
  }

  /// @return the unique thread index.
  inline int threadindex(void) {
    return _thread_index;
  }

  /// @brief Spawn a thread.
  void *spawn(const void     *caller,
              threadFunction *fn,
              void           *arg) {
    // If system is not protected, we should open protection.
    if (!_isCopyOnWrite) {
      setCopyOnWrite(true);
      atomicBegin(caller);
    }

    atomicEnd();

    _memory.finalcommit(true);

    // If fence is already enabled, then we should wait for token to proceed.
    if (_fence_enabled) {
      waitToken();

      // In order to speedup the performance, we try to create as many children
      // as possible once. So we set the _fence_enabled to false now, then
      // current
      // thread don't need to wait on token anymore.
      // Since other threads are either waiting on internal fence or waiting on
      // the parent notification,
      // it will be fine to do so.
      // When current thread are trying to wakeup the children threads, it will
      // set
      // _fence_enabled to true again.
      _fence_enabled = false;
      _children_threads_count = 0;
    }

    _children_threads_count++;

    void *ptr = _thread.spawn(caller, fn, arg, _thread_index);

    // Start a new transaction, even if _thread.spawn failed
    atomicBegin(caller);

    return ptr;
  }

  /// @brief Wait for a thread.
  void join(const void *caller,
            void       *v,
            void       **result) {
    int  child_threadindex = 0;
    bool wakeupChildren = false;

    // Return immediately if the thread argument is NULL.
    if (v == NULL) {
      fprintf(stderr, "%d: join with invalid parameter\n", getpid());
      return;
    }

    // Wait on token if the fence is already started.
    // It is important to maitain the determinism by waiting.
    // No need to wait when fence is not started since join is the first
    // synchronization after spawning, other thread should wait for
    // the notification from me.
    if (_fence_enabled) {
      waitToken();
    }

    atomicEnd();
    _memory.finalcommit(true);

    if (!_fence_enabled) {
      startFence();
      wakeupChildren = true;
    }

    // Get the joinee's thread index.
    child_threadindex = _thread.getThreadIndex(v);

    // When child is not finished, current thread should wait on cond var until
    // child is exited.
    // It is possible that children has been exited, then it will make sure
    // this.
    _determ.join(child_threadindex, _thread_index,
                 wakeupChildren);

    // Release the token.
    putToken();

    // Cleanup some status about the joinee.
    _thread.join(v, result);

    // Now we should wait on fence in order to proceed.
    waitFence();

    // Start next transaction.
    atomicBegin(caller);

    // Check whether we can copy on write at all.
    // If current thread is the only alive thread, then disable copy on write.
    if (_determ.isSingleAliveThread()) {
      setCopyOnWrite(false);

      // Do some cleanup for fence.
      closeFence();
    }
  }

  /// @brief Do a pthread_cancel
  void cancel(const void *caller,
              void       *v) {
    int threadindex;
    bool isFound = false;

    // If I am not holding the token, wait on token to guarantee determinism.
    if (!_token_holding) {
      waitToken();
    }

    atomicEnd();

    // When the thread to be cancel is still there, we are forcing that thread
    // to commit every owned page if we are using lazy commit mechanism.
    // It is important to call this function before _thread.cancel since
    // threadindex or threadpid information will be destroyed _thread.cancel.
    if (isFound) {
      forceThreadCommit(v);
    }
    atomicBegin(caller);
    threadindex = _thread.cancel(v);
    _determ.cancel(threadindex);

    // Put token and wait on fence if I waitToken before.
    if (!_token_holding) {
      putToken();
      waitFence();
    }
  }

  void kill(const void *caller,
            void       *v,
            int        sig) {
    if ((sig == SIGKILL)
        || (sig == SIGTERM)) {
      cancel(caller, v);
    }

    // If I am not holding the token, wait on token to guarantee determinism.
    if (!_token_holding) {
      waitToken();
    }

    atomicEnd();
    _thread.thread_kill(v, sig);

    atomicBegin(caller);

    // Put token and wait on fence if I waitToken before.
    if (!_token_holding) {
      putToken();
      waitFence();
    }
  }

  /* Heap-related functions. */
  inline void *malloc(size_t sz) {
    return _memory.malloc(sz);
  }

  void *calloc(size_t nmemb,
               size_t sz) {
    void *ptr = _memory.malloc(nmemb * sz);

    memset(ptr, 0, nmemb * sz);
    return ptr;
  }

  // In fact, we can delay to open its information about heap.
  inline void free(void *ptr) {
    _memory.free(ptr);
  }

  inline size_t getSize(void *ptr) {
    return _memory.getSize(ptr);
  }

  void *realloc(void   *ptr,
                size_t sz) {
    void *newptr;

    if (ptr == NULL) {
      newptr = _memory.malloc(sz);
      return newptr;
    }

    if (sz == 0) {
      _memory.free(ptr);
      return NULL;
    }

    newptr = _memory.realloc(ptr, sz);

    return newptr;
  }

  void *memalign(size_t align, size_t size) {
    // check if power of 2
    if ((align & - align) != align) {
      errno = EINVAL;
      return NULL;
    }

    if (align <= 4 * sizeof(size_t)) {
      return _memory.malloc(size);
    }

    unsigned char *mem = (unsigned char *)malloc(size + align - 1);

    if (mem == NULL) {
      return NULL;
    }

    unsigned char *aligned =
      (unsigned char *)(((uintptr_t)mem + align - 1) & (-align));

    if (aligned == mem) {
      return mem;
    }

    objectHeader *header = ((objectHeader *)mem) - 1;
    new(header)objectHeader(mem - aligned - sizeof(objectHeader));
    free(mem);

    new(aligned - sizeof(objectHeader))objectHeader(size);

    return aligned;
  }

  void *mmap(void *addr, size_t length, int prot, int flags, int fd,
             off_t offset) {
    return _memory.map(addr, length, prot, flags, fd, offset);
  }

  int munmap(void *addr, size_t len) {
    return _memory.unmap(addr, len);
  }

  ///// conditional variable functions.
  void cond_init(void *cond) {
    _determ.cond_init(cond);
  }

  void cond_destroy(void *cond) {
    _determ.cond_destroy(cond);
  }

  // Barrier support
  int barrier_init(pthread_barrier_t *barrier,
                   unsigned int      count) {
    _determ.barrier_init(barrier, count);

    return 0;
  }

  int barrier_destroy(pthread_barrier_t *barrier) {
    _determ.barrier_destroy(barrier);

    return 0;
  }

  ///// mutex functions
  /// FIXME: maybe it is better to save those actual mutex address in original
  // mutex.
  int mutex_init(pthread_mutex_t *mutex) {
    _determ.lock_init((void *)mutex);

    return 0;
  }

  void startFence(void) {
    ASSERT(_fence_enabled != true);

    // We start fence only if we are have more than two processes.
    ASSERT(_children_threads_count != 0);

    // Start fence.
    _determ.startFence(_children_threads_count);

    _children_threads_count = 0;

    _fence_enabled = true;
  }

  void waitFence(void) {
    _determ.waitFence(_thread_index, false);
  }

  // New optimization here.
  // We will add one parallel commit phase before one can get token
  void waitToken(void) {
    _determ.waitFence(_thread_index, true);

    _determ.getToken();
  }

  // If those threads sending out condsignal or condbroadcast,
  // we will use condvar here.
  void putToken(void) {
    // release the token and pass the token to next.
    _determ.putToken(_thread_index);
  }

  // FIXME: if we are trying to remove atomicEnd() before mutex_lock(),
  // we should unlock() this lock if abort(), otherwise, it will
  // cause the dead-lock().
  void mutex_lock(const void      *caller,
                  pthread_mutex_t *mutex) {
    if (!_fence_enabled) {
      if (_children_threads_count == 0) {
        return;
      } else {
        startFence();

        // Waking up all waiting children
        _determ.notifyWaitingChildren();
      }
    }

    // Calculate how many locks are acquired under the token.
    // Since we treat multiple locks as one lock, we only start
    // the transaction in the beginning and close the transaction
    // when lock_count equals to 0.
    _lock_count++;

    if (_determ.lock_isowner(mutex)
        || _determ.isSingleWorkingThread()) {
      // Then there is no need to acquire the lock.
      bool result = _determ.lock_acquire(mutex);

      if (result == false) {
        goto getLockAgain;
      }
      return;
    } else {
      getLockAgain:

      // If we are not holding the token, trying to get the token in the
      // beginning.
      if (!_token_holding) {
        waitToken();
        _token_holding = true;
        atomicEnd();

        atomicBegin(caller);
      }

      // We are trying to get current lock.
      // Whenver someone didn't release the lock, getLock should be false.
      bool getLock = _determ.lock_acquire(mutex);

      // getLock);
      if (getLock == false) {
        // If we can't get lock, let other threads to move on first
        // in order to maintain the semantics of pthreads.
        // Current thread simply pass the token and wait for
        // next run.
        atomicEnd();
        putToken();
        atomicBegin(caller);
        waitFence();
        _token_holding = false;
        goto getLockAgain;
      }
    }
  }

  void mutex_unlock(const void      *caller,
                    pthread_mutex_t *mutex) {
    if (!_fence_enabled) {
      return;
    }

    // Decrement the lock account
    _lock_count--;


    // Unlock current lock.
    _determ.lock_release(mutex);

    // Since multiple lock are considering as one big lock,
    // we only do transaction end operations when no one is holding the lock.
    // However, when lock is owned, there is no need to close the transaction.
    // But for another case, there is only one thread and not any more(by
    // sending out singal).
    // if(_lock_count == 0 && _token_holding &&
    // !_determ.isSingleWorkingThread())
    if ((_lock_count == 0)
        && _token_holding)
    {
      atomicEnd();
      putToken();
      _token_holding = false;

      atomicBegin(caller);
      waitFence();
    }
  }

  int mutex_destroy(pthread_mutex_t *mutex) {
    _determ.lock_destroy(mutex);

    return 0;
  }

  // Add the barrier support.
  int barrier_wait(pthread_barrier_t *barrier) {
    if (!_fence_enabled) {
      if (_children_threads_count == 0) {
        return 0;
      } else {
        startFence();

        // Waking up all waiting children
        _determ.notifyWaitingChildren();
      }
    }

    waitToken();
    atomicEnd();
    _determ.barrier_wait(barrier, _thread_index);

    return 0;
  }

  // Support for sigwait() functions in order to avoid deadlock.
  int sig_wait(void           *caller,
               const sigset_t *set,
               int            *sig) {
    int ret;

    waitToken();
    atomicEnd();

    ret = _determ.sig_wait(set, sig, _thread_index);

    if (ret == 0) {
      atomicBegin(caller);
    }

    return ret;
  }

  void cond_wait(const void *caller,
                 void       *cond,
                 void       *lock) {
    // corresponding lock should be acquired before.
    ASSERT(_token_holding == true);

    // ASSERT(_determ.lock_is_acquired() == true);
    atomicEnd();

    // We have to release token in cond_wait, otherwise
    // it can cause deadlock!!! Some other threads
    // waiting for the token be no progress at all.
    _determ.cond_wait(_thread_index, cond, lock);
    atomicBegin(caller);
  }

  void cond_broadcast(const void *caller,
                      void       *cond) {
    if (!_fence_enabled) {
      return;
    }

    // If broadcast is sent out under the lock, no need to get token.
    if (!_token_holding) {
      waitToken();
    }

    atomicEnd();
    _determ.cond_broadcast(cond);
    atomicBegin(caller);

    if (!_token_holding) {
      putToken();
      waitFence();
    }
  }

  void cond_signal(const void *caller,
                   void       *cond) {
    if (!_fence_enabled) {
      return;
    }

    if (!_token_holding) {
      waitToken();
    }

    atomicEnd();

    _determ.cond_signal(cond);
    atomicBegin(caller);

    if (!_token_holding) {
      putToken();
      waitFence();
    }
  }

  /// @brief Start a transaction.
  void atomicBegin(const void *caller) {
    fflush(stdout);

    if (!_isCopyOnWrite) {
      return;
    }

    _thread.startThunk();

    _sched.trigger();

    tthread::EventData d;
    d.thunk.id = _thread.getThunkId();
    d.thunk.cpu = _sched.getCPU();

    tthread::logevent e(tthread::logevent::THUNK, caller, d);
    e.setThreadId(_thread_index);
    _logger.add(e);

    // Now start.
    _memory.begin();
    _cpu_time.start();
  }

  /// @brief End a transaction, aborting it if necessary.
  void atomicEnd() {
    fflush(stdout);

    if (!_isCopyOnWrite) {
      return;
    }
    tthread::EventData d;
    d.end.cpu_time = _cpu_time.get();
    _logger.add(tthread::logevent(tthread::logevent::END,
                                  NULL,
                                  d));

    // Commit all private modifications to shared mapping
    _memory.commit();
  }
};
