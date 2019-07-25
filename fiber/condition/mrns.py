from typing import Set, List, Union

from fiber.condition.base import BaseCondition


class MRNS(BaseCondition):
    def __init__(
        self,
        mrns: Union[Set[str], Set[int], List[str], List[int]] = None,
    ):
        if isinstance(mrns, set):
            mrns = list(mrns)
        mrns = set(map(str, mrns))
        self._mrns = mrns or set()

    def __getstate__(self):
        return {
            'class': self.__class__.__name__,
            'attributes': {
                'mrns': sorted(list(self._mrns))
            }
        }
