from functools import reduce

import pandas as pd
import yaml

from fiber import DEFAULT_STORE_FILE_PATH
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

    @classmethod
    def from_condition_store(
        cls,
        name,
        file_path=DEFAULT_STORE_FILE_PATH
    ):
        with open(file_path, 'r') as f:
            diagnosis = yaml.load(f, Loader=yaml.FullLoader)["diagnosis"]
            df = pd.DataFrame.from_dict(diagnosis)

        icd10_codes = reduce(
            cls.__or__,
            [
                cls(context='ICD-10', code=code)
                for code in df[df.name == name]["icd10cm"].iloc[0]
            ]
        )

        icd9_codes = reduce(
            cls.__or__,
            [
                cls(context='ICD-9', code=code)
                for code in df[df.name == name]["icd9cm"].iloc[0]
            ]
        )

        condition = (icd9_codes | icd10_codes)
        condition._label = name
        return condition
