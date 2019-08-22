import pandas as pd

from fiber.condition.fact.fact import FactCondition
from fiber.database.table import (
    d_pers,
    d_uom,
    fact,
    fd_proc,
)


class Procedure(FactCondition):

    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code_column = fd_proc.CONTEXT_PROCEDURE_CODE
    category_column = fd_proc.PROCEDURE_TYPE
    description_column = fd_proc.PROCEDURE_DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]


class Measurement(Procedure):
    dimensions = {'PROCEDURE', 'UNIT_OF_MEASURE'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        fact.TIME_OF_DAY_KEY,
        fd_proc.CONTEXT_NAME,
        fd_proc.CONTEXT_PROCEDURE_CODE,
        fact.VALUE,
        d_uom.UNIT_OF_MEASURE
    ]

    def __init__(self, description: str = '', **kwargs):
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)
        self.condition_operation = None
        self.condition_value = None

    def _fetch_data(self, included_mrns=None, limit=None):
        df = super()._fetch_data(included_mrns, limit=limit)
        df['value'] = pd.to_numeric(
            df.value, errors='coerce'
        )
        return df

    def create_clause(self):
        clause = super().create_clause()
        if self.condition_operation:
            clause &= getattr(fact.VALUE, self.condition_operation)(
                              self.condition_value)
        return clause

    # Defining __eq__ removes __hash__  because the hashes have to be equal
    def __hash__(self):
        """Returns a unique hash for the condition definition."""
        return super().__hash__()

    def to_json(self):
        json = super().to_json()
        # Condition is connected with AND/OR
        if not self.children:
            json['condition'] = {
                'operation': self.condition_operation,
                'value': self.condition_value,
            }
        return json

    def from_json(self, json):
        vitalsign = super().from_json(json)
        getattr(vitalsign, json['condition']['operation'])(
                           json['condition']['value'])
        return vitalsign

    def __lt__(self, other):
        self.condition_operation = '__lt__'
        self.condition_value = other
        return self

    def __le__(self, other):
        self.condition_operation = '__le__'
        self.condition_value = other
        return self

    def __eq__(self, other):
        self.condition_operation = '__eq__'
        self.condition_value = other
        return self

    def __ne__(self, other):
        self.condition_operation = '__ne__'
        self.condition_value = other
        return self

    def __gt__(self, other):
        self.condition_operation = '__gt__'
        self.condition_value = other
        return self

    def __ge__(self, other):
        self.condition_operation = '__ge__'
        self.condition_value = other
        return self

    @property
    def default_aggregations(self):
        return {
            'value': {
                'mean': 'mean', 'min': 'min',
                'max': 'max', 'count': 'count'
            },
            'time_of_day_key': 'min'
        }


class VitalSign(Measurement):
    def __init__(self, description: str = '', **kwargs):
        kwargs['category'] = 'Vital Signs'
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)


class Height(Measurement):
    def __init__(self, **kwargs):
        kwargs['description'] = 'HEIGHT'
        super().__init__(**kwargs)


class Weight(Measurement):
    def __init__(self, **kwargs):
        kwargs['description'] = 'WEIGHT'
        super().__init__(**kwargs)
