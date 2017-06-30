import curses, os, progressbar, multiprocessing, time
from progressbar import ProgressBar
from multiprocessing import Pool

def runner_parallel(inp):
    function, args, name, queue, order_idx = inp
    idx = int(multiprocessing.current_process().name.split("-")[1])
    custom_text = progressbar.FormatCustomText('{} - %(type_op)s: '.format(name), dict(type_op = "Start"))
    widget_style = [custom_text, progressbar.Percentage(), ' (', progressbar.SimpleProgress(), ')', ' ', progressbar.Bar(), ' ', progressbar.Timer(), ' ', progressbar.AdaptiveETA()]
    args += [ProgressBar(widgets = widget_style, fd = Writer((0, idx - 1), queue)), custom_text]
    return (function(*args), order_idx)

class Writer(object):
    def __init__(self, location, queue = None, interface = None):
        self.location = location
        self.queue = queue
        self.interface = interface
    def write(self, string):
        if self.queue is None:
            self.interface.addstr(self.location[1], self.location[0], string)
        else:
            self.queue.put((self.location, string))
    def flush(self):
        if self.interface:
            self.interface.refresh()

def ProgressPool(run_args, processes = 2):
    manager = multiprocessing.Manager()
    the_queue = manager.Queue()
    result = []
    widget_style = ['All: ', progressbar.Percentage(), ' (', progressbar.SimpleProgress(), ')', ' ', progressbar.Bar(), ' ', progressbar.Timer(), ' ', progressbar.AdaptiveETA()]
    run_args = [(func, args, name, the_queue, order_idx) for order_idx, (func, name, args) in enumerate(run_args)]
    if len(run_args) < processes:
        processes = len(run_args)
    if processes == 0:
        return []
    try:
        interface = curses.initscr()
        curses.noecho()
        curses.cbreak()
        main_pb = ProgressBar(widgets = widget_style, fd = Writer((0, 0), interface = interface), max_value=len(run_args))
        with Pool(processes = processes) as pool:
            workers = pool.imap_unordered(runner_parallel, run_args)
            idx = 0
            main_pb.start()
            while True:
                try:
                    try:
                        while True:
                            result.append(workers.next(0))
                            idx += 1
                            main_pb.update(idx)
                    except multiprocessing.TimeoutError:
                        pass
                    while not the_queue.empty():
                        pos, text = the_queue.get()
                        interface.addstr(pos[1], pos[0], text)
                    interface.refresh()
                    time.sleep(1)
                except StopIteration:
                    break
    finally:
        curses.echo()
        curses.nocbreak()
        curses.endwin()
    result = sorted(result, key = lambda x:x[1])
    return [r[0] for r in result]