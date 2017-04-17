#pragma once

/*
 * @file   tthread/logentry.h
 * @brief  a memory event
 */

#include "visibility.h"

class xthread;

namespace tthread {
#pragma pack(push, 1)

typedef union EventData {
  struct {
    // synchronisation point within on thread
    int id;
    int cpu;
  } thunk;
  struct {
    // memory address at which the first page fault was triggered
    const void *address;
  } memory;
  struct {} finish;
  struct {
    // cpu time of a thunk
    long long cpu_time;
  } end;
} Data;

class _PUBLIC_ logevent {
public:

  enum Type {
    INVALID = 0,
    WRITE = 1,
    READ = 2,
    THUNK = 3,
    FINISH = 4,
    END = 5
  };

private:

  // how memory was access (read/write)
  char _type;

  // return address, which issued the first page fault on this page
  const void *_returnAddress;

  // process id, which accessed the memory
  int _threadId;

  Data _data;

public:

  logevent(Type type, const void *returnAddress, Data data) :
    _type(type),
    _returnAddress(returnAddress),
    _threadId(0),
    _data(data)
  {}

  inline void setThreadId(int threadId) {
    _threadId = threadId;
  }

  inline Data& getData() {
    return _data;
  }

  inline Type getType() {
    return (Type)_type;
  }

  inline const void *getReturnAddress() {
    return _returnAddress;
  }

  inline int getThreadId() {
    return _threadId;
  }
};
#pragma pack(pop)
}
