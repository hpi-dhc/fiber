import time


class Timer:

    def __init__(self):
        self.elapsed = -1

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.elapsed = self.end - self.start

    def __repr__(self):
        return f'Timer({self.elapsed:.4f}s)'
