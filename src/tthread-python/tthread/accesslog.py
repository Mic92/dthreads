import os
import struct
from collections import namedtuple
import tthread

default_event_fields = [
        # how memory was access (read/write)
        ("type", "c"),
        # return address, which issued the first page fault on this page
        ("return_address", "Q"),
        # process id, which accessed the memory
        ("thread_id", "i"),
        # event specific data
        # ...
        ]
thunk_event_data = [("id", "i"), ("cpu", "i")]
memory_event_data = [("address", "Q")]
finish_event_data = [("placeholder", "i")]
end_event_data = [("cpu_time", "Q")]

header_fields = [
        # Used to identify log file type
        ("file_magic", "I"),
        # Log format version
        ("version", "I"),
        # Header size in bytes
        ("header_size", "Q"),
        # Number of entries written to the log,
        # the file size might be greater.
        ("event_count", "Q"),
        ("global_start", "Q"),
        ("global_end", "Q"),
        ("heap_start", "Q"),
        ("heap_end", "Q"),
        ]


def make_type(name, fields):
    event = namedtuple(name, " ".join([p[0] for p in fields]))
    fmt = ["="] + [p[1] for p in fields]
    event.fmt = "".join(fmt)
    event.size = struct.calcsize(event.fmt)
    return event

InvalidEvent = make_type("InvalidEvent", [("type", "c")])
WriteEvent = make_type("WriteEvent", default_event_fields + memory_event_data)
ReadEvent = make_type("ReadEvent", default_event_fields + memory_event_data)
ThunkEvent = make_type("ThunkEvent", default_event_fields + thunk_event_data)
FinishEvent = make_type("FinishEvent",
                        default_event_fields + finish_event_data)
EndEvent = make_type("EndEvent", default_event_fields + end_event_data)

events = [InvalidEvent, WriteEvent, ReadEvent, ThunkEvent, FinishEvent, EndEvent]
log_event_size = max([e.size for e in events])

Header = make_type("Header", header_fields)
log_file_magic = 0xC3D2C3D2


class Error(tthread.Error):
    pass


class Log:
    def __init__(self, return_code, log_file):
        self.return_code = return_code
        self.file = log_file

    def read(self):
        self.header = self._read_header()
        for i in range(self.header.event_count):
            event_bytes = self.file.read(log_event_size)
            type_byte = event_bytes[0]
            if type_byte >= len(events):
                msg = "type field '%d' is out of range 0..%d" \
                        % (type_byte, len(events))
                raise Error(msg)
            event = events[type_byte]
            tuples = struct.unpack(event.fmt, event_bytes[:event.size])
            yield event(*tuples)

    def is_heap(self, addr):
        return self.header.heap_start <= addr <= self.header.heap_end

    def is_global(self, addr):
        return self.header.global_start <= addr <= self.header.global_end

    def is_mmap(self, addr):
        return not (self.is_heap(addr) or self.is_global(addr))

    def _read_header(self):
        try:
            stat = os.fstat(self.file.fileno())
        except OSError as e:
            raise Error("failed to determine size of tthread_log: %s" % e)
        if stat.st_size < Header.size:
            msg = "expected tthread_log to be at least %d, got %d" \
                    % (Header.size, stat.st_size)
            raise Error(msg)
        header_bytes = self.file.read(Header.size)
        header = Header(*struct.unpack(Header.fmt, header_bytes))
        self.file.seek(header.header_size)
        if header.file_magic != log_file_magic:
            msg = "expect file_magick of tthread_log " \
                  "to be equal %d, got %d" \
                  % (log_file_magic, header.file_magick)
            raise Error(msg)
        return header

    def close(self):
        self.file.close()
