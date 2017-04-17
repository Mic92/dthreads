import csv
from tthread import accesslog


class TsvWriter:
    header = [
        "type",
        "return_address",
        "thread_id",
        "id/address",
        "heap/global"
    ]

    def __init__(self, log):
        self.log = log

    def write(self, csvfile):
        w = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        w.writerow(self.header)
        for event in self.log.read():
            if type(event) is accesslog.ThunkEvent:
                self._write_thunk(w, event)
            elif type(event) is accesslog.FinishEvent:
                self._write_finish(w, event)
            elif type(event) is accesslog.EndEvent:
                self._write_end(w, event)
            else:
                self._write_access(w, event)

    def _write_end(self, writer, event):
        writer.writerow(("end",
                         event.return_address,
                         event.thread_id,
                         event.cpu_time,
                         "-"))

    def _write_finish(self, writer, event):
        writer.writerow(("finish",
                        event.return_address,
                        event.thread_id,
                        "-", "-"))

    def _write_thunk(self, writer, event):
        writer.writerow(("thunk",
                        event.return_address,
                        event.thread_id,
                        event.id,
                        event.cpu))

    def _write_access(self, writer, event):
        if type(event) is accesslog.WriteEvent:
            t = "write"
        elif type(event) is accesslog.ReadEvent:
            t = "read"
        else:
            return
        if self.log.is_heap(event.address):
            space = "heap"
        elif self.log.is_global(event.address):
            space = "global"
        else:
            space = "mmap"
        writer.writerow((t,
                        event.return_address,
                        event.thread_id,
                        event.address,
                        space))


class Tsv2Writer:
    header = [
        "access",
        "return_address",
        "thread_id",
        "thunk_id",
        "thunk_return_address",
        "heap/global"
    ]

    def __init__(self, log):
        self.log = log

    def write(self, csvfile):
        w = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        w.writerow(self.header)
        threads = {}
        for event in self.log.read():
            if type(event) is accesslog.ThunkEvent:
                threads[event.thread_id] = (event.id, event.return_address)
            else:
                self._write_access(w, event, threads)

    def _write_access(self, writer, event, threads):
        if type(event) is accesslog.WriteEvent:
            a = "write"
        elif type(event) is accesslog.ReadEvent:
            a = "read"
        else:
            return
        thunk = threads.get(event.thread_id, (0, "_start"))
        if self.log.is_heap(event.address):
            space = "heap"
        elif self.log.is_global(event.address):
            space = "global"
        else:
            space = "mmap"
        writer.writerow((a,
                        event.return_address,
                        event.thread_id,
                        thunk[0],
                        thunk[1],
                        space))

class DTLWriter:
    header = [
        "type",
        "return_address",
        "thread_id",
        "id/address",
        "heap/global"
    ]

    class ThunkEvent:
        def __init__(self, event, thread_id, thunk_id, cpu = None, cpu_time = None):
            self.type = type(event)
            self.thread_id = thread_id
            self.thunk_id = thunk_id
            if cpu is not None: self.cpu = cpu
            if cpu_time is not None: self.cpu_time = cpu_time


    def __init__(self, log):
        self.log = log
        # map: thunk -> page
        self.read_set = dict()
        self.write_set = dict()
        self.thunk_order = list()
        self.cur_threads = dict()

    def write(self, csvfile):
        w = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        # w.writerow(self.header)
        for event in self.log.read():
            if type(event) is accesslog.ThunkEvent:
                self._save_thunk(event)
            elif type(event) is accesslog.FinishEvent:
                self._save_finish(event)
            elif type(event) is accesslog.EndEvent:
                self._save_end(event)
            elif type(event) is accesslog.InvalidEvent:
                # WHY do I catch this event at all?
                pass
            else:
                self._save_access(event)
        self._dump(w)

    def _save_thunk(self, event):
        self.cur_threads[event.thread_id] = (event.id)
        new_event = DTLWriter.ThunkEvent(event, event.thread_id,
                                         event.id, cpu = event.cpu)
        self.thunk_order.append(new_event)

    def _save_end(self, event):
        thunk_id = self.cur_threads[event.thread_id]
        new_event = DTLWriter.ThunkEvent(event, event.thread_id, thunk_id,
                                         cpu_time = event.cpu_time)
        self.thunk_order.append(new_event)

    def _save_finish(self, event):
        thunk_id = self.cur_threads[event.thread_id]
        new_event = DTLWriter.ThunkEvent(event, event.thread_id, thunk_id)
        self.thunk_order.append(new_event)

    def _save_access(self, event):
        if type(event) is accesslog.WriteEvent:
            pass
        elif type(event) is accesslog.ReadEvent:
            pass
        else:
            raise Exception("What is this event?")
        if event.thread_id not in self.cur_threads.keys():
            # There are no events generated for thunks with index
            # zero. We just add them automatically
            self.cur_threads[event.thread_id] = 0
        thunk_id = self.cur_threads[event.thread_id]
        thunk = (event.thread_id, thunk_id)
        if type(event) is accesslog.WriteEvent:
            touch_set = self.write_set
        elif type(event) is accesslog.ReadEvent:
            touch_set = self.read_set

        if thunk not in touch_set:
            touch_set[thunk] = list()
        touch_set[thunk].append(event.address)

    def _dump(self, writer):
        for thunk in self.thunk_order:
            if thunk.type is accesslog.FinishEvent:
                self._write_finish(writer, thunk)
            elif thunk.type is accesslog.EndEvent:
                self._write_end(writer, thunk)
            elif thunk.type is accesslog.ThunkEvent:
                self._write_thunk(writer, thunk)

    def _write_end(self, writer, event):
        writer.writerow(("end",
                         event.thread_id,
                         event.thunk_id,
                         event.cpu_time))

    def _write_finish(self, writer, event):
        writer.writerow(("finish",
                         event.thread_id,
                         event.thunk_id))

    def _write_thunk(self, writer, event):
        writer.writerow(("thunk",
                         event.thread_id,
                         event.thunk_id,
                         event.cpu))
        self._write_access(writer, event)

    def _write_access(self, writer, event):
        thunk = (event.thread_id, event.thunk_id)
        if thunk in self.read_set:
            for addr in self.ranges(self.read_set[thunk]):
                writer.writerow(("read",
                                 event.thread_id,
                                 event.thunk_id,
                                 addr[0], addr[1]))
        if thunk in self.write_set:
            for addr in self.ranges(self.write_set[thunk]):
                writer.writerow(("write",
                                 event.thread_id,
                                 event.thunk_id,
                                 addr[0], addr[1]))

    def ranges(self, lst):
        s = e = None
        for i in sorted(lst):
            if s is None:
                s = e = i
            elif i == e or i == e + 1:
                e = i
            else:
                yield (s, e)
                s = e = i
        if s is not None:
            yield (s, e)
