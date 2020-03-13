import time

import fiber


class Timer:
    """
    Own implementation of timer-functionality, circumventing licence-problems
    """

    def __init__(self, name: str = ''):
        """
        Args:
            name: name of the timer to be created
        """
        self.name = name
        self.end = None

    @property
    def elapsed(self):
        """True if the timer is elapsed else False"""
        return (self.end or time.time()) - self.start

    def __enter__(self):
        """
        start the timer

        Returns:
            reference to timer-object
        """
        self.start = time.time()
        return self

    def __exit__(self, *args):
        """
        end the timer

        Args:
            args: not used!

        Returns:
            reference to timer-object
        """
        self.end = time.time()
        if self.name and fiber.config.VERBOSE:
            print(f'{self.name} time: {self.elapsed:.2f}s')

    def __repr__(self):
        """
        string containing the timer's elapsed-status
        (time trimmed to four digits in ms)
        """
        return f'Timer({self.elapsed:.4f}s)'
