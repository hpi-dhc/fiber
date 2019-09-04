from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_diag,
)


class Diagnosis(_FactCondition):
    """
    The Diagnosis adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'DIAGNOSIS' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, CONTEXT_NAME and the code_column in this case respectively.
    """
    dimensions = {'DIAGNOSIS'}
    d_table = fd_diag
    code_column = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category_column = fd_diag.DIAGNOSIS_TYPE
    description_column = fd_diag.DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]
