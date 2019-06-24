from typing import Set


class BaseCondition:
    """The BaseCondition supplies a basic interface to combine sets of mrns.

    """

    def __init__(
        self,
        mrns: Set[str] = None
    ):
        self._mrns = mrns or set()

    @property
    def mrns(self):
        if not self.already_executed:
            self._mrns = self._fetch_mrns()
        return self._mrns

    @property
    def already_executed(self):
        return bool(self._mrns)

    def _fetch_mrns(self) -> Set[str]:
        """Should return a set of mrns for which the condition holds true."""
        raise NotImplementedError

    def __or__(self, other):
        return BaseCondition(self.mrns | other.mrns)

    def __and__(self, other):
        return BaseCondition(self.mrns & other.mrns)
