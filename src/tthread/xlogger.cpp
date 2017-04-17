#include <stddef.h>

#include "tthread/logevent.h"
#include "xatomic.h"
#include "xlogger.h"

void xlogger::add(tthread::logevent e) {
  if (!global_data->enable_logging) {
    return;
  }

  // FIXME later on, _thread should be logged
  // as an seperate event
  if (_thread != NULL) {
    e.setThreadId(_thread->getThreadIndex());
  }

  unsigned long next = xatomic::increment_and_return(_next, 1);
  unsigned long required_size =
    ((next + 1) * EVENT_SIZE) - _mmapOffset;

  if (required_size > REQUEST_SIZE) {
    growLog();
  }

  // substract file offset from _next
  char *byte_offset = ((char *)(_log + next)) - _mmapOffset;
  *((tthread::logevent *)byte_offset) = e;
}
