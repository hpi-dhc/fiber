import pandas as pd
from sqlalchemy import orm, sql

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
        epic_lab.unit_of_measurement
    ]
    mrn_column = epic_lab.MEDICAL_RECORD_NUMBER

    def __init__(
        self,
        name: str = '',
        abnormal: bool = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.abnormal = abnormal

    def to_json(self):
        return {
            'class': self.__class__.__name__,
            'attributes': {
                'name': self.name,
                'abnormal': self.abnormal,
            },
        }

    def create_clause(self):
        clause = sql.true()
        if self.name:
            clause &= _case_insensitive_like(epic_lab.TEST_NAME,
                                             self.name)
        if self.abnormal is not None:
            clause &= (
                epic_lab.ABNORMAL_FLAG == 'Y' if self.abnormal
                else epic_lab.ABNORMAL_FLAG != 'Y'
            )

        return clause

    def create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).with_entities(
            self.mrn_column
        ).distinct()

    def _fetch_data(self, included_mrns=None, limit=None):
        df = super()._fetch_data(included_mrns, limit=limit)
        df['test_result_value'] = pd.to_numeric(
            df.test_result_value, errors='coerce'
        )
        df['abnormal_flag'] = pd.to_numeric(df.abnormal_flag == 'Y')
        df.dropna(inplace=True)

        df['test_name'] = df['test_name'].astype('category')
        df['result_flag'] = df['result_flag'].astype(
            'category'
        )
        return df

    @property
    def default_aggregations(self):
        return {
            'test_result_value': 'mean',
            'abnormal_flag': 'any',
            'result_flag': lambda x: pd.Series.mode(x)[0]
        }
