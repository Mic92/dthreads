#include <stdexcept>

#include <stdio.h>
#include <papi.h>
#include <pthread.h>
#include <sched.h>

#include <sys/types.h>
#include <unistd.h>

#include "xsched.h"

#define errExit(msg)    do { perror(msg); exit(EXIT_FAILURE);   \
  } while (0)

void xsched::trigger()
{
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(_thread->getThunkId() % 4, &set);
  if (sched_setaffinity(getpid(), sizeof(set), &set) == -1)
    errExit("sched_setaffinity");
}

int xsched::getCPU()
{
  return sched_getcpu();
}
