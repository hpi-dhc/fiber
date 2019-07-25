from typing import Set
from cachetools import cached
import json


# They can get very large (implement own version of Cachtools Cache)
mrn_cache = {}
data_cache = {}


def _hash_request(instance, for_mrns={}, limit=None):
    return hash(str(hash(instance)) + str(hash(frozenset(for_mrns)))
                + str(limit))


class BaseCondition:
    """The BaseCondition supplies a basic interface to combine sets of mrns.

    """
    OR = 'or'
    AND = 'and'

    def __init__(
        self,
        mrns: Set[str] = None,
        children=None,
        operator=None,
    ):
        self._mrns = mrns or set()
        self.children = children
        self.operator = operator

    @cached(cache=mrn_cache, key=_hash_request)
    def get_mrns(self, limit=None):
        if not self._mrns:
            self._mrns = self._fetch_mrns(limit=limit)
        return self._mrns

    def _fetch_mrns(self, limit=None) -> Set[str]:
        """Should return a set of mrns for which the condition holds true."""
        raise NotImplementedError

    @cached(cache=data_cache, key=_hash_request)
    def get_data(self, for_mrns, limit=None):
        return self._fetch_data(for_mrns, limit=limit)

    def _fetch_data(self, limit=None):
        """Must be implemented by subclass to return relevant data."""
        raise NotImplementedError

    def __hash__(self):
        """Returns a unique hash for the condition definition."""
        json_str = json.dumps(self.to_json()).encode("utf-8")
        return hash(json_str)

    def to_json(self):
        return self.__getstate__()

    def from_json(self, json):
        return self.__setstate__(json)

    def __getstate__(self):
        """ Should be implemented by subclasses.

        Returns a json representation of the Condition.
        This is also used for caching previous results and should not return
        a different json if the condtion execution returns the same results.
        """
        return {
            self.operator: [c.to_json() for c in self.children]
        }

    def __setstate__(self, dict):
        """
        Should load the Conditions based on the json.
        """
        raise NotImplementedError

    def __or__(self, other):
        return BaseCondition(
                mrns=self.get_mrns() | other.get_mrns(),
                children=[self, other],
                operator=BaseCondition.OR,
            )

    def __and__(self, other):
        return BaseCondition(
                mrns=self.get_mrns() & other.get_mrns(),
                children=[self, other],
                operator=BaseCondition.AND,
            )

    def __len__(self):
        return len(self.get_mrns())
