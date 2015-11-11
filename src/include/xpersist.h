#pragma once

/*
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
 * @file   xpersist.h
 * @brief  Main file to handle page fault, commits.
 * @author Emery Berger <http://www.cs.umass.edu/~emery>
 * @author Tongping Liu <http://www.cs.umass.edu/~tonyliu>
 * @author Charlie Curtsinger <http://www.cs.umass.edu/~charlie>
 */

#include <climits>
#include <list>
#include <map>
#include <set>
#include <vector>

#if !defined(_WIN32)
# include <sys/mman.h>
# include <sys/types.h>
# include <unistd.h>
#endif // if !defined(_WIN32)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xmmintrin.h>

#include "heaplayers/ansiwrapper.h"
#include "heaplayers/freelistheap.h"
#include "xatomic.h"

#include "heaplayers/stlallocator.h"
#include "privateheap.h"
#include "xbitmap.h"
#include "xdefines.h"

#include "xlogger.h"

#include "debug.h"

#include "unused.h"
#include "xpageentry.h"

#if defined(sun)
extern "C" int madvise(caddr_t addr,
                       size_t  len,
                       int     advice);
#endif // if defined(sun)

/**
 * @class xpersist
 * @brief Makes a range of memory persistent and consistent.
 */
template<class Type, unsigned long NElts = 1>
class xpersist {
public:

  enum { SHARED_PAGE = INT_MAX };

  enum page_access_info {
    PAGE_ACCESS_NONE = 0,
    PAGE_ACCESS_READ = 1,
    PAGE_ACCESS_READ_WRITE = 4,
    PAGE_UNUSED = 8
  };


  /// @arg startaddr  the optional starting address of the local memory.
  xpersist(void *startaddr = 0, size_t startsize = 0)
    : _startaddr(startaddr),
    _startsize(startsize),
    _isHeap(startaddr == NULL)
  {
    // Check predefined globals size is large enough or not.
    if (_startsize > 0) {
      if (_startsize > size()) {
        fprintf(stderr,
                "This persistent region (%ld) is too small (%ld).\n",
                size(),
                _startsize);
        ::abort();
      }
    }

    // Get a temporary file name (which had better not be NFS-mounted...).
    char _backingFname[L_tmpnam];
    sprintf(_backingFname, _isHeap ? "tthreadHXXXXXX" : "tthreadGXXXXXX");
    _backingFd = mkstemp(_backingFname);

    if (_backingFd == -1) {
      fprintf(stderr, "Failed to make persistent file.\n");
      ::abort();
    }

    // Set the files to the sizes of the desired object.
    if (ftruncate(_backingFd, size())) {
      fprintf(stderr, "Mysterious error with ftruncate.NElts %ld\n", NElts);
      ::abort();
    }

    // Get rid of the files when we exit.
    unlink(_backingFname);

    char _versionsFname[L_tmpnam];

    // Get another temporary file name (which had better not be NFS-mounted...).
    sprintf(_versionsFname, "tthreadVXXXXXX");
    _versionsFd = mkstemp(_versionsFname);

    if (_versionsFd == -1) {
      fprintf(stderr, "Failed to make persistent file.\n");
      ::abort();
    }

    if (ftruncate(_versionsFd, TotalPageNums * sizeof(unsigned long))) {
      // Some sort of mysterious error.
      // Adios.
      fprintf(stderr,
              "Mysterious error with ftruncate. TotalPageNums %d\n",
              TotalPageNums);
      ::abort();
    }

    unlink(_versionsFname);

    //
    // Establish two maps to the backing file.
    // The persistent map (shared maping) is shared.
    _persistentMemory = (Type *)WRAP(mmap)(NULL, size(), PROT_READ
                                           | PROT_WRITE, MAP_SHARED, _backingFd,
                                           0);

    if (_persistentMemory == MAP_FAILED) {
      fprintf(stderr, "arguments: start= %p, length=%ld\n",
              (void *)NULL, size());
      perror("Persistent memory creation:");
      ::abort();
    }

    // If we specified a start address (globals), copy the contents into the
    // persistent area now because the transient memory map is going
    // to squash it.
    if (!_isHeap) {
      memcpy(_persistentMemory, _startaddr, _startsize);
    }

    // The transient map is private and optionally fixed at the desired start
    // address for globals.
    // In order to get the same copy with _persistentMemory for those
    // constructor stuff,
    // we will set to MAP_PRIVATE at first, then memory protection will be
    // opened in initialize().
    _transientMemory = (Type *)WRAP(mmap)(_startaddr, size(),
                                          PROT_READ | PROT_WRITE,
                                          MAP_SHARED |
                                          (_startaddr != NULL ? MAP_FIXED : 0),
                                          _backingFd, 0);

    if (_transientMemory == MAP_FAILED) {
      fprintf(stderr, "arguments = %p, %ld, %d, %d, %d\n",
              _startaddr, size(), PROT_READ | PROT_WRITE,
              MAP_SHARED | (_startaddr != NULL ? MAP_FIXED : 0), _backingFd);
      perror("Transient memory creation:");
      ::abort();
    }

    _isCopyOnWrite = false;

    DEBUGF("xpersist intialize: transient = %p, persistent = %p, size = %zx",
           _transientMemory,
           _persistentMemory,
           size());

    // We are trying to use page's version number to speedup the commit phase.
    _persistentVersions = (volatile unsigned long *)WRAP(mmap)(NULL,
                                                               TotalPageNums *
                                                               sizeof(unsigned
                                                                      long),
                                                               PROT_READ |
                                                               PROT_WRITE,
                                                               MAP_SHARED,
                                                               _versionsFd,
                                                               0);

    _pageUsers =
      (struct shareinfo *)WRAP(mmap)(NULL,
                                     TotalPageNums * sizeof(struct shareinfo),
                                     PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_ANONYMOUS,
                                     -1,
                                     0);

    _pageOwner =
      (volatile int *)WRAP(mmap)(NULL,
                                 TotalPageNums * sizeof(size_t),
                                 PROT_READ | PROT_WRITE,
                                 MAP_SHARED | MAP_ANONYMOUS,
                                 -1,
                                 0);

    // Local
    _pageInfo = (unsigned long *)WRAP(mmap)(NULL,
                                            TotalPageNums * sizeof(size_t),
                                            PROT_READ | PROT_WRITE,
                                            MAP_PRIVATE | MAP_ANONYMOUS,
                                            -1,
                                            0);

    _ownedblockinfo = (unsigned long *)WRAP(mmap)(NULL,
                                                  xdefines::PageSize,
                                                  PROT_READ | PROT_WRITE,
                                                  MAP_PRIVATE | MAP_ANONYMOUS,
                                                  -1,
                                                  0);

    if ((_pageOwner == MAP_FAILED)
        || (_pageInfo == MAP_FAILED)) {
      fprintf(stderr,
              "xpersist: mmap about ownership tracking error with %s\n",
              strerror(errno));

      // If we couldn't map it, something has seriously gone wrong. Bail.
      ::abort();
    }

    if ((_transientMemory == MAP_FAILED)
        || (_persistentVersions == MAP_FAILED)
        || (_pageUsers == MAP_FAILED)
        || (_persistentMemory == MAP_FAILED)) {
      fprintf(stderr, "xpersist: mmap error with %s\n", strerror(errno));

      // If we couldn't map it, something has seriously gone wrong. Bail.
      ::abort();
    }

#ifdef GET_CHARACTERISTICS
    _pageChanges = (struct pagechangeinfo *)WRAP(mmap)(NULL,
                                                       TotalPageNums *
                                                       sizeof(struct
                                                              pagechangeinfo),
                                                       PROT_READ | PROT_WRITE,
                                                       MAP_SHARED |
                                                       MAP_ANONYMOUS,
                                                       -1,
                                                       0);
#endif // ifdef GET_CHARACTERISTICS
  }

  void initialize(xlogger& logger) {
    _logger = &logger;

    // A string of one bits.
    allones = _mm_setzero_si128();
    allones = _mm_cmpeq_epi32(allones, allones);

    // Clean the ownership.
    _dirtiedPagesList.clear();
  }

  void finalize() {
    if (_isCopyOnWrite) {
      setProtection(base(), size(), PROT_READ | PROT_WRITE, MAP_SHARED);
    }

#ifdef GET_CHARACTERISTICS
    int pages = getSingleThreadPages();
    fprintf(stderr, "Totally there are %d single thread pages.\n", pages);
#endif // ifdef GET_CHARACTERISTICS
  }

#ifdef GET_CHARACTERISTICS
  int getSingleThreadPages() {
    int pages = 0;

    for (int i = 0; i < TotalPageNums; i++) {
      struct pagechangeinfo *page = (struct pagechangeinfo *)&_pageChanges[i];

      if ((page->version > 1)
          && (page->tid != 0xFFFF)) {
        //    fprintf(stderr, "i %d with tid %d and version %d\n", i, page->tid,
        // page->version);
        pages += page->version;
      }
    }

    return pages;
  }

#endif // ifdef GET_CHARACTERISTICS

  // enable memory protection
  // @end if memory is heap, this address points to end of allocated memory
  // @copyOnWrite if true writes will not be shared across processes
  void setCopyOnWrite(void *end, bool copyOnWrite) {
    int writeSemantic = copyOnWrite ? MAP_PRIVATE : MAP_SHARED;

    // We will set everything to PROT_NONE
    // For globals, all pages are set to SHARED_PAGE in the beginning.
    // For heap, only those allocated pages are set to SHARED_PAGE.
    // Those pages that haven't allocated are set to be PRIVATE at first.
    if (_isHeap) {
      int allocSize = (intptr_t)end - (intptr_t)base();

      setProtection(base(), size(), PROT_NONE, writeSemantic);

      for (int i = 0; i < allocSize / xdefines::PageSize; i++) {
        _pageOwner[i] = SHARED_PAGE;
        _pageInfo[i]  = PAGE_ACCESS_NONE;
      }

      // Those un-allocated pages can be owned.
      for (int i = allocSize / xdefines::PageSize; i < TotalPageNums; i++) {
        _pageOwner[i] = 0;
        _pageInfo[i] = PAGE_UNUSED;
      }
    } else {
      setProtection(base(), size(), PROT_READ, writeSemantic);

      for (int i = 0; i < TotalPageNums; i++) {
        _pageOwner[i] = SHARED_PAGE;
        _pageInfo[i] = PAGE_ACCESS_READ;
      }
    }

    _ownedblocks = 0;
    _trans = 0;
    _isCopyOnWrite = copyOnWrite;
  }

  // disable memory protection, writes to memory will affect all processes and
  // pagefault handler will be disabled
  void closeProtection() {
    setProtection(base(), size(), PROT_READ | PROT_WRITE, MAP_SHARED);
    _isCopyOnWrite = false;
  }

  void setThreadIndex(int index) {
    _threadindex = index;
  }

  /// @return true iff the address is in this space.
  bool inRange(void *addr) {
    if (((size_t)addr >= (size_t)base())
        && ((size_t)addr
            < (size_t)base() + size())) {
      return true;
    } else {
      return false;
    }
  }

  /// @return the start of the memory region being managed.
  inline Type *base() const {
    return _transientMemory;
  }

  bool mem_write(void *addr, void *val) {
    unsigned long offset = (intptr_t)addr - (intptr_t)base();
    void **ptr = (void **)((intptr_t)_persistentMemory + offset);

    *ptr = val;

    // fprintf(stderr, "addr %p val %p(int %d) with ptr %p\n", addr, val,
    // (unsigned long)val, *ptr);
    return true;
  }

  /// @return the size in bytes of the underlying object.
  static inline size_t size() {
    return NElts * sizeof(Type);
  }

  // Change the page to read-only mode.
  void mprotectRead(void *addr, int pageNo) {
    _pageInfo[pageNo] = PAGE_ACCESS_READ;
    int res = mprotect(addr, xdefines::PageSize, PROT_READ);
	if (res != 0) {
		fprintf(stderr,
				"Failed to set page read-only: mprotect(%p, %d, %d): %s\n",
				addr,
				xdefines::PageSize,
				PROT_READ,
				strerror(errno));
		::abort();
	}
  }

  // Change the page to r/w mode.
  void mprotectReadWrite(void *addr, int pageNo) {
    if (_pageOwner[pageNo] == getpid()) {
      _pageInfo[pageNo] = PAGE_ACCESS_READ_WRITE;
    }
    int res = mprotect(addr, xdefines::PageSize, PROT_READ | PROT_WRITE);
	if (res != 0) {
		fprintf(stderr,
				"Failed to set page read-write: mprotect(%p, %d, %d): %s\n",
				addr,
				xdefines::PageSize,
				PROT_READ|PROT_WRITE,
				strerror(errno));
		::abort();
	}
  }

  inline bool isSharedPage(int pageNo) {
    return _pageOwner[pageNo] == (int)SHARED_PAGE;
  }

  // Those owned page will also set to MAP_PRIVATE and READ_ONLY
  // in the beginning. The difference is that they don't need to commit
  // immediately in order to reduce the time of serial phases.
  // The function will be called when one thread is getting a new superblock.
  void setOwnedPage(void *addr, size_t size) {
    if (!_isCopyOnWrite) {
      return;
    }

    int pid = getpid();
    size_t startPage = computePage((intptr_t)addr - (intptr_t)base());
    size_t pages = size / xdefines::PageSize;
    int blocks = _ownedblocks;

    mprotect(addr, size, PROT_NONE);

    for (size_t i = startPage; i < startPage + pages; i++) {
      _pageOwner[i] = pid;
      _pageInfo[i] = PAGE_ACCESS_NONE;
    }

    // This block are now owned by current thread.
    // In the end, all pages in this block are going to be checked.
    _ownedblockinfo[blocks * 2] = startPage;
    _ownedblockinfo[blocks * 2 + 1] = startPage + pages;
    _ownedblocks++;

    if (_ownedblocks == xdefines::PageSize / 2) {
      fprintf(stderr, "Not enought to hold super blocks.\n");
      exit(-1);
    }
  }

  // @ Page fault handler
  void handleAccess(void *addr, bool is_write, const void *issuerAddress) {
    // Compute the page number of this item
    int pageNo = computePage((size_t)addr - (size_t)base());
    unsigned long *pageStart =
      (unsigned long *)((intptr_t)_transientMemory + xdefines::PageSize *
                        pageNo);

    auto t = is_write ? tthread::logevent::WRITE :
             tthread::logevent::READ;
    tthread::EventData m;

    m.memory.address = addr;
    _logger->add(tthread::logevent(t, issuerAddress, m));

    if (is_write) {
      handleWrite(pageNo, pageStart);
    } else {
      handleRead(pageNo, pageStart);
    }
  }

  bool nop() {
    return _dirtiedPagesList.empty();
  }

  /// @brief Start a transaction.
  void begin() {
    // Update all pages related in this dirty page list
    updateAll();
  }

#ifndef SSE_SUPPORT
  void commitWord(char *src, char *twin, char *dest) {
    for (int i = 0; i < sizeof(long long); i++) {
      if (src[i] != twin[i]) {
        dest[i] = src[i];
      }
    }
  }

#endif // ifndef SSE_SUPPORT

  // Write those difference between local and twin to the destination.
  void writePageDiffs(const void *local, const void *twin,
                      void *dest) {
#ifdef SSE_SUPPORT

    // Now we are using the SSE3 instructions to speedup the commits.
    __m128i *localbuf = (__m128i *)local;
    __m128i *twinbuf = (__m128i *)twin;
    __m128i *destbuf = (__m128i *)dest;

    // Some vectorizing pragamata here; not sure if gcc implements them.
    for (unsigned long i = 0; i < xdefines::PageSize / sizeof(__m128i); i++) {
      __m128i localChunk, twinChunk;

      localChunk = _mm_load_si128(&localbuf[i]);
      twinChunk = _mm_load_si128(&twinbuf[i]);

      // Compare the local and twin byte-wise.
      __m128i eqChunk = _mm_cmpeq_epi8(localChunk, twinChunk);

      // Invert the bits by XORing them with ones.
      __m128i neqChunk = _mm_xor_si128(allones, eqChunk);

      // Write local pieces into destbuf everywhere diffs.
      _mm_maskmoveu_si128(localChunk, neqChunk, (char *)&destbuf[i]);
    }
#else // ifdef SSE_SUPPORT

    /* If hardware can't support SSE3 instructions, use slow commits as
       following. */
    long long *mylocal = (long long *)local;
    long long *mytwin = (long long *)twin;
    long long *mydest = (long long *)dest;

    for (int i = 0; i < xdefines::PageSize / sizeof(long long); i++) {
      if (mylocal[i] != mytwin[i]) {
        commitWord((char *)&mylocal[i], (char *)&mytwin[i], (char *)&mydest[i]);

        // if(mytwin[i] != mydest[i] && mylocal[i] != mydest[i])
        // fprintf(stderr, "%d: RACE at %p from %x to %x (dest %x). pageno
        // %d\n", getpid(), &mylocal[i], mytwin[i], mylocal[i], mydest[i],
        // pageno);
      }
    }
#endif // ifdef SSE_SUPPORT
  }

  // Create the twin page for the page with specified pageNo.
  void createTwinPage(int pageNo) {
    int index;
    unsigned long *twin;
    struct shareinfo *shareinfo = NULL;

    // We will use a multiprocess-shared array(_pageUsers) to save twin page
    // information.
    shareinfo = &_pageUsers[pageNo];

    index = xbitmap::getInstance().get();

    // We can never get bitmap index with 0.
    ASSERT(index != 0);

    shareinfo->bitmapIndex = index;

    // Create the "shared-twin-page" for them
    twin = (unsigned long *)xbitmap::getInstance().getAddress(index);
    memcpy(twin,
           (void *)((intptr_t)_persistentMemory + xdefines::PageSize * pageNo),
           xdefines::PageSize);

    INC_COUNTER(twinpage);

    // Set the twin page version number.
    xbitmap::getInstance().setVersion(index, _persistentVersions[pageNo]);
  }

#ifdef GET_CHARACTERISTICS
  void recordPageChanges(int pageNo) {
    struct pagechangeinfo *page =
      (struct pagechangeinfo *)&_pageChanges[pageNo];
    unsigned short tid = page->tid;

    int mine = getpid();

    // If this word is not shared, we should set to current thread.
    if (tid == 0) {
      page->tid = mine;
    } else if ((tid != 0)
               && (tid != mine)
               && (tid != 0xFFFF)) {
      // This word is shared by different threads. Set to 0xFFFF.
      page->tid = 0xFFFF;
    }

    page->version++;
  }

#else // ifdef GET_CHARACTERISTICS
  void recordPageChanges(int pageNo) {
    UNUSED(pageNo);
  }

#endif // ifdef GET_CHARACTERISTICS

  // This function will force the process with specified pid to commit all
  // owned-by-it pages.
  // This happens when one thread are trying to call pthread_kill or
  // pthread_cancel to kill one thread.
  void forceCommitOwnedPages(int pid, void *end) {
    size_t startpage = 0;
    size_t endpage = ((intptr_t)end - (intptr_t)base()) / xdefines::PageSize;

    // Check all possible pages.
    for (size_t i = startpage; i < endpage; i++) {
      // When one page is owned by specified thread,
      if (_pageOwner[i] == pid) {
        notifyOwnerToCommit(i);
      }
    }
  }

  void notifyOwnerToCommit(int pageNo) {
    // Get the owner information.
    unsigned int owner = _pageOwner[pageNo];

    if (owner == SHARED_PAGE) {
      // owner has committed page, we can exit now.
      return;
    }

    // Otherwise, we should send a signal to the owner.
    union sigval val;
    val.sival_int = pageNo;
    int i;

    notify_owner:
    i = 0;

    // if never owned, try to get it, otherwise signal owner
    if (_pageOwner[pageNo] == 0) {
      setSharedPage(pageNo);
    } else if (sigqueue(owner, SIGUSR1, val) != 0) {
      setSharedPage(pageNo);
      return;
    }

    // Spin here until the page is set to be SHARED; Ad hoc synchronization.
    while (!isSharedPage(pageNo)) {
      i++;

      if (i == 100000) {
        goto notify_owner;
      }
    }
  }

  void setSharedPage(int pageNo) {
    if (_pageOwner[pageNo] != (int)SHARED_PAGE) {
      xatomic::exchange((unsigned long *)&_pageOwner[pageNo], SHARED_PAGE);
      _pageInfo[pageNo] = PAGE_ACCESS_READ;
    }
  }

  inline void cleanupOwnedBlocks() {
    _ownedblocks = 0;
  }

  void commitOwnedPage(int pageNo, bool setShared) {
    // Get corresponding entry.
    void *addr = (void *)((intptr_t)base() + pageNo * xdefines::PageSize);
    void *share =
      (void *)((intptr_t)_persistentMemory + xdefines::PageSize * pageNo);

#ifdef GET_CHARACTERISTICS
    recordPageChanges(pageNo);
#endif // ifdef GET_CHARACTERISTICS
    INC_COUNTER(dirtypage);
    INC_COUNTER(lazypage);


    // Commit its previous version, if it has changed
    if ((_pageInfo[pageNo] != PAGE_ACCESS_NONE)
        && (_pageInfo[pageNo] != PAGE_ACCESS_READ)) {
      memcpy(share, addr, xdefines::PageSize);
      mprotect(addr, xdefines::PageSize, PROT_NONE);
    }

    if (setShared) {
      // Finally, we should set this page to SHARED state.
      setSharedPage(pageNo);

      // We also release the private copy when one page is already shared.
      madvise(addr, xdefines::PageSize, MADV_DONTNEED);
    }

    // Update the version number.
    _persistentVersions[pageNo]++;
  }

  // Commit all pages when the thread is going to exit
  void finalcommit(bool release) {
    int blocks = _ownedblocks;
    int startpage;
    int endpage;
    int j;

    // Only checked owned blocks.
    for (int i = 0; i < blocks; i++) {
      startpage = _ownedblockinfo[i * 2];
      endpage = _ownedblockinfo[i * 2 + 1];

      if (release) {
        for (j = startpage; j < endpage; j++) {
          if (_pageOwner[j] == getpid()) {
            commitOwnedPage(j, true);
          }
        }
      } else {
        // We do not release the private copy when one thread is exit in order
        // to improve the performance.
        for (j = startpage; j < endpage; j++) {
          if (_pageOwner[j] == getpid()) {
            commitOwnedPage(j, false);
          }
        }
      }
    }
  }

  // Get the start address of specified page.
  inline void *getPageStart(int pageNo) {
    return (void *)((intptr_t)base() + pageNo * xdefines::PageSize);
  }

  // Commit local modifications to shared mapping
  void checkandcommit() {
    struct shareinfo *shareinfo = NULL;
    struct xpageinfo *pageinfo = NULL;
    int pageNo;
    unsigned long *share;
    unsigned long *local;
    int mypid = getpid();

    INC_COUNTER(commit);

    if (_dirtiedPagesList.size() == 0) {
      return;
    }

    _trans++;

    // Check all pages in the dirty list
    for (dirtyListType::iterator i = _dirtiedPagesList.begin();
         i != _dirtiedPagesList.end(); ++i) {
      bool isModified = false;
      pageinfo = (struct xpageinfo *)i->second;
      pageNo = pageinfo->pageNo;

      // Get the shareinfo and persistent address.
      shareinfo = &_pageUsers[pageNo];
      share =
        (unsigned long *)((intptr_t)_persistentMemory + xdefines::PageSize *
                          pageNo);
      local = (unsigned long *)pageinfo->pageStart;

      // When there are multiple writers on the page and the twin page is not
      // created (bitmapIndex = 0).
      if ((shareinfo->users > 1)
          && (shareinfo->bitmapIndex == 0)) {
        createTwinPage(pageNo);
      }

      // When there are multiple writes on this page, the page cannot be
      // owned.
      // When this page is not owned, then we do commit.
      if ((shareinfo->users != 1)
          || (_pageOwner[pageNo] != mypid)) {
        pageinfo->release = true;

        // If the version is the same as shared, use memcpy to commit.
        if (pageinfo->version == _persistentVersions[pageNo]) {
          memcpy(share, local, xdefines::PageSize);
        } else {
          // slow commits
          unsigned long *twin =
            (unsigned long *)xbitmap::getInstance().getAddress(
              shareinfo->bitmapIndex);
          ASSERT(shareinfo->bitmapIndex != 0);

          recordPageChanges(pageNo);
          INC_COUNTER(slowpage);

          // Use the slower page commit, comparing to "twin".
          setSharedPage(pageNo);
          writePageDiffs(local, twin, share);
        }

        isModified = true;
      } else {
        // in case we own the page,
        // it should not be released
        // by during atomicBegin()
        pageinfo->release = false;
      }

      if (isModified) {
        if (shareinfo->users == 1) {
          // If I am the only user, release the share information.
          shareinfo->bitmapIndex = 0;
        }

        // Now there is one less user on this page.
        shareinfo->users--;

        INC_COUNTER(dirtypage);

        // Update the version number.
        _persistentVersions[pageNo]++;
      }
    }
  }

  /// @brief Update every page frame from the backing file if necessary.
  void updateAll() {
    struct xpageinfo *pageinfo;

    // Dump in-updated page frame for safety!!!
    dirtyListType::iterator i;

    for (i = _dirtiedPagesList.begin(); i != _dirtiedPagesList.end(); ++i) {
      pageinfo = (struct xpageinfo *)i->second;

      updatePage(pageinfo->pageStart, 1, pageinfo->release);
    }

    // Now there is no need to use dirtiedPagesList any more
    _dirtiedPagesList.clear();
    xpageentry::getInstance().cleanup();
  }

  /// @brief Commit all writes.
  void memoryBarrier() {
    xatomic::memoryBarrier();
  }

private:

  void *setProtection(void *start, size_t size, int prot, int flags) {
    void *area;
    size_t offset = (intptr_t)start - (intptr_t)base();

    // Map to readonly private area.
    area = (Type *)WRAP(mmap)(start, size, prot, flags | MAP_FIXED,
                              _backingFd, offset);

    if (area == MAP_FAILED) {
      fprintf(stderr,
              "Change protection failed for pid %d, start %p, size %ld: %s\n",
              getpid(),
              start,
              size,
              strerror(errno));
      exit(EXIT_FAILURE);
    }
    return area;
  }

  inline size_t computePage(size_t index) {
    return (index * sizeof(Type)) / xdefines::PageSize;
  }

  /// @brief Update the given page frame from the backing file.
  void updatePage(void *local, size_t pages, bool release) {
    if (release) {
      madvise(local, xdefines::PageSize * pages, MADV_DONTNEED);
    }

    // reset page protection
    // keep globals at is at the moment, to avoid double page fault in page
    // fault handler
    int protection = _isHeap ? PROT_NONE : PROT_READ;
    mprotect(local, xdefines::PageSize * pages, protection);
  }

  void handleRead(int pageNo, unsigned long *pageStart) {
    switch (_pageInfo[pageNo]) {
    case PAGE_UNUSED: // When we are trying to access other-owned page.

      // Current page must be owned by other pages.
      notifyOwnerToCommit(pageNo);
      break;

    case PAGE_ACCESS_NONE:
      break;

    default:
      ASSERT(0); // invalid state -> BUG!
    }

    // read a page the first time
    mprotectRead(pageStart, pageNo);
  }

  void handleWrite(int pageNo, unsigned long *pageStart) {
    // Check the access type of this page.
    switch (_pageInfo[pageNo]) {
    case PAGE_UNUSED: // When we are trying to access other-owned page.
      // Current page must be owned by other pages.
      notifyOwnerToCommit(pageNo);

      mprotectReadWrite(pageStart, pageNo);

      // Now page is writable, add this page to dirty set.
      break;

    case PAGE_ACCESS_READ: // write to a page the first time
    case PAGE_ACCESS_NONE:

      // There are two cases here.
      // (1) I have read other's owned page, now I am writing on it.
      // (2) I tries to write to my owned page in the first, but without
      // previous version.
      mprotectReadWrite(pageStart, pageNo);

      // Now page is writable, add this page to dirty set.
      break;

    case PAGE_ACCESS_READ_WRITE:

      // One case, I am trying to write to those dirty pages again.
      mprotectReadWrite(pageStart, pageNo);

      // Since we are already wrote to this page before, now we are trying to
      // write to this page again. Now we should commit old version to the
      // shared copy. record access to owned page
      commitOwnedPage(pageNo, false);

    default:
      ASSERT(0); // invalid state
    }

    // page is set SHARED, so just write through
    if (!_isCopyOnWrite) {
      return;
    }

    // just record access to owned pages, they will be commited at the end
    if (_pageOwner[pageNo] == getpid()) {
      return;
    }

    // Now one more user are using this page.
    xatomic::increment((unsigned long *)&_pageUsers[pageNo]);

    // Add this page to the dirty set.
    struct xpageinfo *curr = NULL;
    curr = xpageentry::getInstance().alloc();
    curr->pageNo = pageNo;
    curr->pageStart = (void *)pageStart;
    curr->release = true;
    curr->version = _persistentVersions[pageNo];

    // Then add current page to dirty list.
    _dirtiedPagesList.insert(std::pair<int, void *>(pageNo, curr));
  }

  xlogger *_logger;

  /// The starting address of the region.
  void *const _startaddr;

  /// The size of the region.
  const size_t _startsize;

  /// True if current xpersist.h is a heap.
  bool _isHeap;

  typedef std::pair<int, void *>objType;

  // The objects are pairs, mapping void * pointers to sizes.
  typedef HL::STLAllocator<objType, privateheap>dirtyListTypeAllocator;

  typedef std::less<int>localComparator;

  typedef std::multimap<int, void *, localComparator,
                        dirtyListTypeAllocator>dirtyListType;

  /// A map of dirtied pages.
  dirtyListType _dirtiedPagesList;

  /// The file descriptor for the backing store.
  int _backingFd;

  /// The transient (not yet backed) memory.
  Type *_transientMemory;

  /// The persistent (backed to disk) memory.
  Type *_persistentMemory;

  bool _isCopyOnWrite;

  /// The file descriptor for the versions.
  int _versionsFd;

  /// The version numbers that are backed to disk.
  volatile unsigned long *_persistentVersions;

  unsigned int _trans;

  // Every time when we are getting a super block, we will update this
  // information.
  // Then it is used to reduce the checking time in final commit. We only
  // checked those
  // blocks owned by me.
  unsigned long *_ownedblockinfo;
  unsigned long _ownedblocks; // How manu blocks are owned by myself.

  // This array are used to save access type and the pointer for previous copy.
  unsigned long *_pageInfo;

  volatile int *_pageOwner;

  /// Local version numbers for each page.
  __m128i allones;

  struct shareinfo {
    volatile unsigned short users;
    volatile unsigned short bitmapIndex;
  };

  struct shareinfo *_pageUsers;


  /// The length of the version array.
  enum {  TotalPageNums = sizeof(Type) * NElts / xdefines::PageSize };

#ifdef GET_CHARACTERISTICS
  struct pagechangeinfo {
    unsigned short tid;
    unsigned short version;
  };
  struct pagechangeinfo *_pageChanges;
#endif // ifdef GET_CHARACTERISTICS

  int _threadindex;
};
