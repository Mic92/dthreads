#include <stdexcept>

#include <stdio.h>
#include <papi.h>
#include <pthread.h>

#include <sys/types.h>
#include <unistd.h>

#include "xtime.h"

xtime::xtime()
{
}

void xtime::stop()
{
}

void xtime::init()
{
  _cpu_time = 1;

  int retval;

  // Initialize the library
  retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT && retval > 0) {
    throw std::runtime_error("PAPI library version mismatch!\n");
  }
  if (retval < 0)
    throw std::runtime_error("Failed to initialize PAPI library.\n");
  retval = PAPI_is_initialized();
  if (retval != PAPI_LOW_LEVEL_INITED)
    throw std::runtime_error("Failed to initialize PAPI library.\n");

  if (PAPI_set_debug(PAPI_VERB_ECONT) != PAPI_OK)
    throw std::runtime_error("Failed to set up verbosity level.\n");
}

void xtime::start()
{
  if (PAPI_is_initialized() == PAPI_NOT_INITED)
    init();
  _cpu_time=PAPI_get_virt_usec();
}

long long xtime::get()
{
  if (PAPI_is_initialized() == PAPI_NOT_INITED)
    init();
  long long end = PAPI_get_virt_usec();
  return end - _cpu_time;
}
