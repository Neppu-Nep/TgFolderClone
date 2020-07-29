import os
from sys import stdout
from collections import deque
from progress import Infinite
try:
    from time import monotonic
except ImportError:
    from time import time as monotonic


class CounterProgress(Infinite):
    file = stdout

    def is_tty(self):
        return False

    def write(self, s):
        if self.file:
            line = self.message + s.ljust(self._width)
            print('\r' + line, end='', file=self.file)
            self._width = max(self._width, len(s))
            self.file.flush()

    def finish(self):
        if self.file:
            print(file=self.file)

    def __init__(self, message='', **kwargs):
        self.encoding = kwargs.get('encoding', 'cp1252')
        super(CounterProgress, self).__init__(**kwargs)
        self.index = 0
        self.start_ts = monotonic()
        self.avg = 0
        self._avg_update_ts = self.start_ts
        self._ts = self.start_ts
        self._xput = deque(maxlen=self.sma_window)
        self.max = kwargs.get('max', 100)
        for key, val in kwargs.items():
            setattr(self, key, val)

        self._width = 0
        self.message = message

        if 'DYNO' not in os.environ:
            self.file.reconfigure(encoding=self.encoding)

        if self.file:
            print(self.message, end='', file=self.file)
            self.file.flush()

    @property
    def progress(self):
        return min(1, self.index / self.max)

    @property
    def percent(self):
        return self.progress * 100

    def update(self):
        self.write(" - {:.2f}% [{}/{}]".format(self.percent, self.index, self.max))

    def finish_update_with_error(self, count):
        self.write(" | Synced ({} files failed to copy)".format(count))

    def finish_update(self):
        self.write(" | Synced")
