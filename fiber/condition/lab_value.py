from typing import Optional, Set

import pandas as pd
from sqlalchemy import orm

from fiber.condition import _DatabaseCondition
from fiber.condition.database import _case_insensitive_like
from fiber.condition.mixins import (
    AgeMixin,
    ComparisonMixin,
)
from fiber.database.table import epic_lab


class LabValue(AgeMixin, ComparisonMixin, _DatabaseCondition):
    """
    LabValues are part of the building-blocks of FIBER. In order to define
    Cohorts, LabValues are basically the access-point to the results received
    from Labs when one had to perform tests outside of the hospital.

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
    age_column = epic_lab.AGE_IN_DAYS
    code_column = epic_lab.TEST_CODE
    description_column = epic_lab.TEST_NAME

    def __init__(
        self,
        name: str = '',
        abnormal: bool = None,
        **kwargs
    ):
        """
        Args:
            String name:
                This is the name of the selected test (e.g. 'GLUCOSE',
                'HEMOGLOBIN')
            Bool abnormal: If this is set the lab values will either only
                contain values that are with in the expected range for a
                healthy person (``abnormal=False``) or only return values that
                are abnormal (``abnormal=True``)

        The string parameters are used in the SQL-LIKE statement after being
        converted to uppercase. This means that  ``%``, ``_`` and  ``[]`` can
        be used to more precisly select patients.
        """
        super().__init__(**kwargs)
        self._attrs['name'] = name
        self._attrs['abnormal'] = abnormal

    def _create_clause(self):
        clause = super()._create_clause()
        if self._attrs['name']:
            clause &= _case_insensitive_like(
                self.description_column, self._attrs['name'])
        if self._attrs['abnormal'] is not None:
            clause &= (
                epic_lab.ABNORMAL_FLAG == 'Y' if self._attrs['abnormal']
                else epic_lab.ABNORMAL_FLAG != 'Y'
            )

        return clause

    def _create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).with_entities(
            self.mrn_column
        ).distinct()

    def _fetch_data(self,
                    included_mrns: Optional[Set] = None,
                    limit: Optional[int] = None):
        """
        LabValue overwrites ``._fetch_data()`` to simplify the result data.
        """
        df = super()._fetch_data(included_mrns, limit=limit)

        if 'abnormal_flag' in df.columns:
            df['abnormal_flag'] = pd.to_numeric(df.abnormal_flag == 'Y')
        if 'test_name' in df.columns:
            df['test_name'] = df['test_name'].astype('category')
        if 'result_flag' in df.columns:
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
