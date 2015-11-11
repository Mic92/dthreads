#pragma once

#include <sys/mman.h>
#include <vector>

#include "heaplayers/stlallocator.h"
#include "privateheap.h"
#include "xdefines.h"

class accessedmmappages {
  typedef HL::STLAllocator<void *, privateheap>privateAllocator;
  typedef std::vector<void *, privateAllocator>pages;

  pages _pages;

public:

  void add(void *addr) {
    // TODO: sorted insert and mprotect over continous memory to reduce syscalls
    _pages.push_back(addr);
  }

  void reset() {
    for (pages::const_iterator it = _pages.begin(); it != _pages.end(); ++it) {
      int res = mprotect(*it, xdefines::PageSize, PROT_NONE);
      if (res != 0) {
          fprintf(stderr,
                  "Failed to reset page protection mprotect(%p, %d, %d): %s\n",
                  *it,
                  xdefines::PageSize,
                  PROT_NONE,
                  strerror(errno));
          ::abort();
      }
    }
    _pages.clear();
  }
};
