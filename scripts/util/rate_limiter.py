# -*- coding: utf-8 -*-
import time, random
class RateLimiter:
    def __init__(self, reqs_per_sec: float = 0.7):
        self.min_interval = 1.0 / max(reqs_per_sec, 0.01)
    def wait(self):
        time.sleep(self.min_interval * (0.9 + 0.2 * random.random()))
