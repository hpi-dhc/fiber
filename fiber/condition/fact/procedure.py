from typing import Optional

from fiber.condition.fact.fact import _FactCondition
from fiber.condition.mixins import ComparisonMixin
from fiber.database.table import (
    d_pers,
    d_uom,
    fact,
    fd_proc,
)


class Procedure(_FactCondition):
    """
    Procedures are parts of the building-blocks of FIBER. In order to define
    Cohorts, Procedures help fetching patients that, for example, undergone
    heart surgery identified by, e.g., ICD-9 code.

    The Procedure adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'PROCEDURE' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, CONTEXT_NAME and the code_column in this case respectively.
    """
    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code_column = fd_proc.CONTEXT_PROCEDURE_CODE
    context_column = d_table.CONTEXT_NAME
    category_column = fd_proc.PROCEDURE_TYPE
    description_column = fd_proc.PROCEDURE_DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        context_column,
        code_column
    ]


class Measurement(ComparisonMixin, Procedure):
    """
    Measurements are part of the building-blocks of FIBER. In order to define
    Cohorts, Measurements help fetching the patients that had some specific
    measurement performed on them, like serum-creatinine-values. In many cases
    there exist subclasses for specific purposes. Make sure to check them out
    before using this and having to adjust stuff by yourself.

    The Measurement adds functionality to the Procedure. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'PROCEDURE' (and optionally age-constraints on the dates as well as
    comparisons mixed in).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, TIME_OF_DAY_KEY, CONTEXT_NAME, CONTEXT_PROCEDURE_CODE,
    NUMERIC_VALUE AND UNIT_OF_MEASURE.
    """

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

    def __init__(self, description: Optional[str] = '', **kwargs):
        """
        Args:
            description: can be any string to search for.
                Can contain %-Wildcards.
            kwargs: arguments that shall be passed higher in the hierarchy
        """
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)

    @property
    def default_aggregations(self):
        """
        This returns the default-aggregations: 'mean', 'min', 'max' and 'count'
        for the value, as well as the 'min' for the time_of_day_key.
        Returns:
            dictionary containing the specified aggregations
        """
        return {
            'value': {
                'mean': 'mean', 'min': 'min',
                'max': 'max', 'count': 'count'
            },
            'time_of_day_key': 'min'
        }


class VitalSign(Measurement):
    """
    VitalSigns are part of the building-blocks of FIBER. In order to define
    Cohorts, VitalSigns help fetching the patients that had some specific
    measurement performed on them, like temperature.

    The VitalSign adds functionality to the Measurement. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'PROCEDURE' (and optionally age-constraints on the dates as well as
    comparisons mixed in).

    The default-columns to return, MEDICAL_RECORD_NUMBER, AGE_IN_DAYS,
    TIME_OF_DAY_KEY, CONTEXT_NAME, CONTEXT_PROCEDURE_CODE, NUMERIC_VALUE AND
    UNIT_OF_MEASURE are defined in the super-class.
    """
    def __init__(self, description: Optional[str] = '', **kwargs):
        """
        Args:
            description: can be any string to search for.
                Can contain %-Wildcards.
            kwargs: arguments that shall be passed higher in the hierarchy
        """
        kwargs['category'] = 'Vital Signs'
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)


class Height(Measurement):
    """
    The Height adds functionality to the VitalSign in order to allow easy
    search for height-measurements.
    """
    def __init__(self, **kwargs):
        """
        Args:
            kwargs: arguments that shall be passed higher in the hierarchy
        """
        kwargs['description'] = 'HEIGHT'
        super().__init__(**kwargs)


class Weight(Measurement):
    """
    The Weight adds functionality to the VitalSign in order to allow easy
    search for weight-measurements.
    """
    def __init__(self, **kwargs):
        """
        Args:
            kwargs: arguments that shall be passed higher in the hierarchy
        """
        kwargs['description'] = 'WEIGHT'
        super().__init__(**kwargs)
