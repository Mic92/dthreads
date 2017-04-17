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
 * @file xdefines.h
 * @brief Some definitions which maybe modified in the future.
 * @author Emery Berger <http://www.cs.umass.edu/~emery>
 * @author Tongping Liu <http://www.cs.umass.edu/~tonyliu>
 * @author Charlie Curtsinger <http://www.cs.umass.edu/~charlie>
 */

#include <sys/types.h>

#if defined(__APPLE__)

// We are going to use the Mach-O substitutes for _end, etc.,
// despite the strong admonition not to. Beware.
# include <mach-o/getsect.h>
#endif // if defined(__APPLE__)

#include "prof.h"

struct xlogger_shared_data {
  volatile off_t fileSize;
  pthread_mutex_t truncateMutex;
};

typedef struct runtime_data {
  volatile unsigned long thread_index;
  struct xlogger_shared_data xlogger;
  struct runtime_stats stats;
  bool enable_logging;
  bool protect_mmap;
} runtime_data_t;

extern runtime_data_t *global_data;

class xdefines {
public:

  enum { STACK_SIZE = 1024 * 1024 };              // 1 * 1048576 };
  // enum { PROTECTEDHEAP_SIZE = 1048576UL * 2048}; // FIX ME 512 };
#ifdef X86_32BIT
  enum { PROTECTEDHEAP_SIZE = 1048576UL * 1024 }; // FIX ME 512 };
#else // ifdef X86_32BIT
  enum { PROTECTEDHEAP_SIZE = 1048576UL * 4096 }; // FIX ME 512 };
#endif // ifdef X86_32BIT
  enum { PROTECTEDHEAP_CHUNK = 10485760 };

  enum { MAX_GLOBALS_SIZE = 1048576UL * 40 };
  enum { INTERNALHEAP_SIZE = 1048576UL * 100 }; // FIXME 10M
  enum { PageSize = 4096UL };
  enum { PageShift = 12UL };
  enum { PAGE_SIZE_MASK = (PageSize - 1) };
  enum { NUM_HEAPS = 32 };                      // was 16
  enum { LOCK_OWNER_BUDGET = 10 };
};

extern "C" {
#if !defined(__APPLE__)
extern int __data_start;
extern int _end;

extern char _etext;
extern char _edata;
#endif // if !defined(__APPLE__)
}

// Macros to align to the nearest page down and up, respectively.
#define PAGE_ALIGN_DOWN(x) (((size_t)(x)) & ~xdefines::PAGE_SIZE_MASK)
#define PAGE_ALIGN_UP(x) ((((size_t)(x)) + xdefines::PAGE_SIZE_MASK) & \
                          ~xdefines::PAGE_SIZE_MASK)

// Macros that define the start and end addresses of program-wide globals.
#if defined(__APPLE__)

# define GLOBALS_START PAGE_ALIGN_DOWN(((size_t)get_etext() - 1))
# define GLOBALS_END PAGE_ALIGN_UP(((size_t)get_end() - 1))

#else // if defined(__APPLE__)

# define GLOBALS_START PAGE_ALIGN_DOWN((size_t)&__data_start)
# define GLOBALS_END PAGE_ALIGN_UP(((size_t)&_end - 1))

#endif // if defined(__APPLE__)

#define GLOBALS_SIZE (GLOBALS_END - GLOBALS_START)
