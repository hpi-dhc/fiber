from fiber.condition.fact.fact import FactCondition
from fiber.condition.mixins import ComparisonMixin
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


class Measurement(ComparisonMixin, Procedure):

    dimensions = {'PROCEDURE', 'UNIT_OF_MEASURE'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        fact.TIME_OF_DAY_KEY,
        fd_proc.CONTEXT_NAME,
        fd_proc.CONTEXT_PROCEDURE_CODE,
        fact.NUMERIC_VALUE,
        d_uom.UNIT_OF_MEASURE
    ]

    def __init__(self, description: str = '', **kwargs):
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)

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
