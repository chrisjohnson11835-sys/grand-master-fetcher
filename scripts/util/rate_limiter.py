import time, random
class RateLimiter:
    def __init__(self, spacing_seconds: float = 1.5):
        self.spacing = max(0.0, spacing_seconds); self._last = 0.0
    def wait(self, jitter_range=(0.0,0.0)):
        now=time.time(); due=self._last+self.spacing
        if now<due: time.sleep(due-now)
        if jitter_range and (jitter_range[0]>0 or jitter_range[1]>0):
            time.sleep(random.uniform(jitter_range[0], jitter_range[1]))
        self._last=time.time()
