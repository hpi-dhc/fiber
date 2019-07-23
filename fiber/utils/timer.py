import time


class Timer:

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.elapsed = self.end - self.start

    def __repr__(self):
        elapsed = getattr(self, 'elapsed', -1)
        return f'Timer({elapsed:.4f}s)'
