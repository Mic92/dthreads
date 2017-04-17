#pragma once

#include <assert.h>
#include <vector>

#include "xdefines.h"
class mmappage {
private:

  void *_addr;
  int _fd, _protection;

  void handleWrite(void *addr) const {
    if (_protection & PROT_WRITE) {
      int res = mprotect(_addr, xdefines::PageSize, PROT_WRITE);
      ASSERT(res == 0);
    } else {
      fprintf(stderr, "Illegal write mmap allocation at %p", addr);
      ::abort();
    }
  }

  void handleRead(void *addr) const {
    if (_protection & PROT_READ) {
      int res = mprotect(addr, xdefines::PageSize, PROT_READ);
      ASSERT(res == 0);
    } else {
      fprintf(stderr, "Illegal read of mmap allocation at %p", addr);
      ::abort();
    }
  }

public:

  explicit mmappage(void *ptr) : _addr(ptr), _fd(-1), _protection(0) {}

  mmappage(void *addr, int fd, int protection) :
    _addr(addr), _fd(fd), _protection(protection) {}

  bool operator<(const mmappage& rhs) const {
    return _addr < rhs._addr;
  }

  const void *addr() const {
    return _addr;
  }

  void handleAccess(xlogger  & logger,
                    void       *addr,
                    bool       isWrite,
                    const void *issuerAddress) const {
    auto t = isWrite ? tthread::logevent::WRITE :
             tthread::logevent::READ;
    tthread::EventData m;

    m.memory.address = (void *)((uintptr_t)addr >> xdefines::PageShift);

    tthread::logevent e(t, issuerAddress, m);

    logger.add(e);
    void *pageaddr = (void *)PAGE_ALIGN_DOWN(addr);
    isWrite ? handleWrite(pageaddr) : handleRead(pageaddr);
  }
};
