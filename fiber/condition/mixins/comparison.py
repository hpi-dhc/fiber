from abc import ABC


class ComparisonMixin(ABC):

    def create_clause(self):
        clause = super().create_clause()

        if 'comp_operator' in self._attrs:
            clause &= getattr(
                self.base_table.NUMERIC_VALUE,
                self._attrs['comp_operator']
            )(self._attrs['comp_value'])

        return clause

    @classmethod
    def from_dict(cls, json):
        obj = super().from_dict(json)
        obj._attrs['comp_operator'] = json['attributes']['comp_operator']
        obj._attrs['comp_value'] = json['attributes']['comp_value']
        return obj

    # Defining __eq__ requires explicit definition of __hash__
    def __hash__(self):
        """Returns a unique hash for the condition definition."""
        return super().__hash__()


def _build_method(name):
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
