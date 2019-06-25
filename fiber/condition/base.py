from typing import Set


class BaseCondition:
    """The BaseCondition supplies a basic interface to combine sets of mrns.

    """

    def __init__(
        self,
        mrns: Set[str] = None
    ):
        self._cached_mrns = mrns or set()

    def get_mrns(self):
        if not self._cached_mrns:
            self._cached_mrns = self._fetch_mrns()
        return self._cached_mrns

    def _fetch_mrns(self) -> Set[str]:
        """Should return a set of mrns for which the condition holds true."""
        raise NotImplementedError

    def get_data(self, for_mrns):
        """Must be implemented by subclass to return relevant data."""
        raise NotImplementedError

    def __or__(self, other):
        return BaseCondition(self.get_mrns() | other.get_mrns())

    def __and__(self, other):
        return BaseCondition(self.get_mrns() & other.get_mrns())
