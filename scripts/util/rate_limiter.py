import random, time
class RateLimiter:
    def __init__(self, spacing_seconds: float = 1.5):
        self.spacing = float(spacing_seconds); self._next = 0.0
    def wait(self, jitter_range=(0.2, 0.6)):
        now = time.time()
        if now < self._next: time.sleep(self._next - now)
        j = random.uniform(*jitter_range) if jitter_range and len(jitter_range)==2 else 0.0
        self._next = time.time() + self.spacing + j
