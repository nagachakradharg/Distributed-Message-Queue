import random
import time
from threading import Timer


class ResettableTimer:
    def __init__(self, function, interval_lb=100, interval_ub=200):
        self.interval = (interval_lb, interval_ub)
        self.function = function
        self.time_interval = self._interval()
        self.timer = Timer(self.time_interval, self.function)

    def _interval(self):
        return random.randint(*self.interval) / 1000

    def run(self):
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.time_interval = self._interval()
        self.timer = Timer(self.time_interval, self.function)
        self.timer.start()

    def pause(self):
        self.timer.cancel()

    def get_time_interval(self):
        return self.time_interval