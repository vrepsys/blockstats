import time
import logging

class Timer:

    def __init__(self, total_jobs, thread_id=1):
        self._total_jobs = total_jobs
        self._done_jobs = 0
        self._start_time = None
        self._last_start = None
        self._last_took = None
        self._thread_id = thread_id

    def start(self):
        self._start_time = time.time()
        self._last_start = self._start_time

    def log(self):
        if self._done_jobs > 0:
            now = time.time()
            total_elapsed = now - self._start_time
            avg = total_elapsed / self._done_jobs
            time_left = avg * (self._total_jobs-self._done_jobs)
            long_duration = ' (LONG) ' if self._last_took > 2 else ' '
            logging.info('thread:%s %s/%s last: %.3fs%savg: %.3fs left: %s',
                         self._thread_id, self._done_jobs, self._total_jobs,
                         self._last_took, long_duration, avg, _format_interval(time_left))
        else:
            logging.info('Starting with %s items', self._total_jobs)

    def tick(self):
        self._last_took = time.time() - self._last_start
        self._last_start = time.time()
        self._done_jobs += 1

def _format_interval(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return '%dh:%02dm:%02ds' % (h, m, s)
