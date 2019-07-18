import pandas as pd
from sqlalchemy import orm

from fiber.database.table import epic_lab
from fiber.condition import DatabaseCondition
from fiber.condition.database import _case_insensitive_like


class LabValue(DatabaseCondition):

    base_table = epic_lab
    _default_columns = [
        epic_lab.MEDICAL_RECORD_NUMBER,
        epic_lab.AGE_IN_DAYS,
        epic_lab.TEST_NAME,
        epic_lab.ABNORMAL_FLAG,
        epic_lab.RESULT_FLAG,
        epic_lab.TEST_RESULT_VALUE,
    ]
    mrn_column = epic_lab.MEDICAL_RECORD_NUMBER

    def __init__(
        self,
        test_name: str = '',
        **kwargs
    ):
        super().__init__(**kwargs)

        if test_name:
            self.clause &= _case_insensitive_like(epic_lab.TEST_NAME,
                                                  test_name)

    def create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).with_entities(
            self.mrn_column
        ).distinct()

    def get_data(self, inclusion_mrns=None, limit=None):
        df = super().get_data(inclusion_mrns, limit=limit)
        prev = len(df)
        df['numeric_value'] = pd.to_numeric(
            df.test_result_value, errors='coerce')
        df.dropna(inplace=True)
        df['abnormal'] = df.abnormal_flag == 'Y'
        print(f'Removed {prev-len(df)} non-numeric values.')
        return df
