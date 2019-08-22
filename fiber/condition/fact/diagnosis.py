from fiber.condition.fact.fact import FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_diag,
)


class Diagnosis(FactCondition):

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
