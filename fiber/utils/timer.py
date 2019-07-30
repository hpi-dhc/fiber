import time


class Timer:

    def __init__(self, name: str = ''):
        self.name = name
        self.end = None

    @property
    def elapsed(self):
        return (self.end or time.time()) - self.start

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        if self.name:
            print(f'{self.name} time: {self.elapsed:.2f}s')

    def __repr__(self):
        return f'Timer({self.elapsed:.4f}s)'
