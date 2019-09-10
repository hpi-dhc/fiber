from abc import ABC
from typing import Optional

from fiber.condition.base import _BaseCondition


class ComparisonMixin(ABC):

    def __init__(self, *args, discard_nans: Optional[bool] = True, **kwargs):
        super().__init__(*args, **kwargs)
        self._attrs['discard_nans'] = discard_nans

    def _create_clause(self):
        clause = super()._create_clause()

        if self._attrs['discard_nans']:
            clause &= (self.base_table.NUMERIC_VALUE.isnot(None))

        # Not the reason for the speedup; just there to prevent issues
        # for tests without a single numeric value.
        clause &= (self.base_table.NUMERIC_VALUE.isnot(None))

        if 'comp_operator' in self._attrs:
            clause &= getattr(
                self.base_table.NUMERIC_VALUE,
                self._attrs['comp_operator']
            )(self._attrs['comp_value'])

        return clause

    @classmethod
    def from_dict(cls: _BaseCondition, json: dict):
        obj = super().from_dict(json)
        if 'comp_operator' in json['attributes']:
            obj._attrs['comp_operator'] = json['attributes']['comp_operator']
            obj._attrs['comp_value'] = json['attributes']['comp_value']
        return obj

    # Defining __eq__ requires explicit definition of __hash__
    def __hash__(self):
        """Returns a unique hash for the condition definition."""
        return super().__hash__()


def _build_method(name: str):
    def operator_method(self, other):
        if 'comp_operator' in self._attrs:
            raise NotImplementedError(
                'Chaining of multiple comparisons not supported.')
        self._attrs['comp_operator'] = name
        self._attrs['comp_value'] = other

        return self
    return operator_method


for operator_method in (
    '__gt__',
    '__lt__',
    '__le__',
    '__ge__',
    '__eq__',
    '__ne__',
):
    method = _build_method(operator_method)
    setattr(ComparisonMixin, operator_method, method)
