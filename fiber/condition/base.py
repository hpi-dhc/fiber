from typing import Set
from cachetools import cached
import json


# They can get very large (implement own version of Cachetools Cache)
mrn_cache = {}
data_cache = {}


def _hash_request(instance, included_mrns=None, limit=None):
    return hash(
        str(hash(instance)) +
        str(hash(frozenset(included_mrns or []))) +
        str(limit)
    )


class BaseCondition:
    """
    The BaseCondition supplies a basic interface to combine conditional
    statements. It is the base to create custom conditions with further
    functionality.
    """
    OR = 'or'
    AND = 'and'

    def __init__(
        self,
        mrns: Set[str] = None,
        children=None,
        operator=None,
    ):
        """
        :param Set mrns: Set of MRN-Strings for which the condition is true.
        :param List children:
            List of child conditions which were combined with an operator.
        :param String operator:
            String representing the combination of the child condition
            (e.g. ``BaseCondition.AND``)
        """
        self._mrns = mrns or set()
        self.children = children
        self.operator = operator

    @cached(cache=mrn_cache, key=_hash_request)
    def get_mrns(self, limit=None):
        """Fetches the mrns of a condition and returns them"""
        if not self._mrns:
            self._mrns = self._fetch_mrns(limit=limit)
        return self._mrns

    def _fetch_mrns(self, limit=None) -> Set[str]:
        """
        Must be implemented by subclasses to return a set of MRNs for which
        the condition holds true. This is called by ``.get_mrns()``
        """
        raise NotImplementedError

    @cached(cache=data_cache, key=_hash_request)
    def get_data(self, included_mrns=None, limit=None):
        """
        Fetches data based on patients defined via this condition and the
        patients given with ``included_mrns``. For each of the patients this
        returns data specified by the condition.

        Not every condition has data to return.
        """
        return self._fetch_data(included_mrns, limit=limit)

    def _fetch_data(self, included_mrns=None, limit=None):
        """
        Can be implemented by subclasses to return relevant data dependant on
        the condition. This is called by ``.get_data()``
        """
        raise NotImplementedError

    def __hash__(self):
        """
        Returns a unique hash for the condition definition. The hash is used to
        cache results of every executed query of the current session.

        Therefore the hash is based of the json representation of the condition
        as this would result in the same conditions and database query.
        """
        return hash(json.dumps(self.to_json()))

    def to_json(self):
        """
        Must be implemented by subclasses to return a json representation of
        the condition.

        Example:

        >>> Patient(gender='Male').to_json()
        {class:'Patient', attributes:{gender:'Male', religion:None, race:None}

        In the BaseCondition the json of subclasses is combined with the
        operators connecting them.

        >>> (Patient(...) & Patient(...)).to_json()
        {'and':[{class:'Patient',...}, {class:'Patient',...}]}

        The ``.to_json()`` function is used for caching previous results and
        should return the same json if the condtion execution returns the same
        results.
        """
        return {
            self.operator: [c.to_json() for c in self.children]
        }

    def from_json(self, json):
        """
        Loads a single condition based on the json by instantiating them with
        the `attributes` defined in the json.

        If other data apart from the class attributes are used the subclass
        needs to implement `.from_json()` to restore the condition accordingly.

        This is used to restore the full condition from a json in
        ``fiber.utils.storage.json_to_condition()``
        """
        return self.__class__(**json['attributes'])

    def __or__(self, other):
        """
        Condition objects allow combination with the ``|`` symbol. On the base
        level this happens via ``Set`` operations on the MRNs that are fetched
        for each condition individually.
        """
        return BaseCondition(
                mrns=self.get_mrns() | other.get_mrns(),
                children=[self, other],
                operator=BaseCondition.OR,
            )

    def __and__(self, other):
        """
        Condition objects allow combination with the ``&`` symbol. On the base
        level this happens via ``Set`` operations on the MRNs that are fetched
        for each condition individually.
        """
        return BaseCondition(
                mrns=self.get_mrns() & other.get_mrns(),
                children=[self, other],
                operator=BaseCondition.AND,
            )

    def __len__(self):
        return len(self.get_mrns())
