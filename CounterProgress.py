from sys import stdout
from progress import Infinite

class CounterProgress(Infinite):
    file = stdout
    sma_window = 10
    check_tty = False
    hide_cursor = False

    def __init__(self, *args, **kwargs):
        super(CounterProgress, self).__init__(*args, **kwargs)
        self.max = kwargs.get('max', 100)

    @property
    def progress(self):
        return min(1, self.index / self.max)

    @property
    def percent(self):
        return self.progress * 100

    def update(self):
        self.write(" - {:.2f}% [{}/{}]".format(self.percent, self.index, self.max))

    def finishupdate(self):
        self.write(" | Synced")
