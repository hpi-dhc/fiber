import pandas as pd
from sqlalchemy import orm

from fiber.database.table import epic_lab
from fiber.condition import DatabaseCondition
from fiber.condition.mixins import (
    AgeMixin,
    ComparisonMixin,
)
from fiber.condition.database import _case_insensitive_like


class LabValue(AgeMixin, ComparisonMixin, DatabaseCondition):
    """
    LabValue is based of Database condition and accesses the EPIC_LAB table it
    contains information about laboratory test which are done for instance on
    blood values.
    """
    base_table = epic_lab
    _default_columns = [
        epic_lab.MEDICAL_RECORD_NUMBER,
        epic_lab.AGE_IN_DAYS,
        epic_lab.TEST_NAME,
        epic_lab.ABNORMAL_FLAG,
        epic_lab.RESULT_FLAG,
        epic_lab.NUMERIC_VALUE,
        epic_lab.unit_of_measurement
    ]
    mrn_column = epic_lab.MEDICAL_RECORD_NUMBER

    def __init__(
        self,
        name: str = '',
        abnormal: bool = None,
        **kwargs
    ):
        """
        :param String name:
            This is the name of the selected test (e.g. 'GLUCOSE',
            'HEMOGLOBIN')
        :param Bool abnormal:
            If this is set the lab values will either only contain values that
            are with in the expected range for a helthy person
            (``abnormal=False``) or only return values that are abnormal
            (``abnormal=True``)

        The string parameters are used in the SQL-LIKE statement after being
        converted to uppercase. This means that  ``%``, ``_`` and  ``[]`` can
        be used to more precisly select patients.
        """
        super().__init__(**kwargs)
        self._attrs['name'] = name
        self._attrs['abnormal'] = abnormal

    def create_clause(self):
        clause = super().create_clause()
        if self._attrs['name']:
            clause &= _case_insensitive_like(
                epic_lab.TEST_NAME, self._attrs['name'])
        if self._attrs['abnormal'] is not None:
            clause &= (
                epic_lab.ABNORMAL_FLAG == 'Y' if self._attrs['abnormal']
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
        """
        LabValue overwirtes ``._fetch_data()`` to simplify the result data.
        """
        df = super()._fetch_data(included_mrns, limit=limit)

        df['abnormal_flag'] = pd.to_numeric(df.abnormal_flag == 'Y')

        df['test_name'] = df['test_name'].astype('category')
        df['result_flag'] = df['result_flag'].astype(
            'category'
        )
        return df

    @property
    def default_aggregations(self):
        return {
            'numeric_value': 'mean',
            'abnormal_flag': 'any',
            'result_flag': lambda x: pd.Series.mode(x)[0]
        }
