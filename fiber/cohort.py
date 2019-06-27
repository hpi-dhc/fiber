from collections.abc import Iterable
from collections import defaultdict
from functools import reduce

import pandas as pd

from fiber.condition.base import BaseCondition
from fiber.condition import DatabaseCondition
from fiber.database import read_with_progress
from fiber.database.mysql import engine as _mysql_engine


class Cohort:

    def __init__(self, condition: BaseCondition):

        self._condition = condition
        self._lab_results = None
        self._excluded_mrns = set()

    def mrns(self):
        return self._condition.get_mrns() - self._excluded_mrns

    def get(self, data_conditions):
        if not isinstance(data_conditions, Iterable):
            data_conditions = [data_conditions]

        data = []

        database_cond = defaultdict(list)
        for c in data_conditions:
            if isinstance(c, DatabaseCondition):
                database_cond[c.base_table].append(c)

        # (TODO) What might be better: Return a list of DataFrames for every
        # data condition and don't merge them:
        #
        # for c in data_conditions:

        for c in database_cond.values():
            c = reduce(DatabaseCondition.__or__, c)
            print(f'Fetching data for {c}..')
            data.append(c.get_data(self.mrns()))

        return data if len(data) > 1 else data[0]

    def exclude(self, mrn_list):
        self._excluded_mrns = self._excluded_mrns | set(mrn_list)
        return self

    def occurs(
        self,
        target, relative_to,
        trim_func=lambda x: x.split('.')[0]
    ):

        event_df = self.get(relative_to)
        target_df = self.get(target)
        event_df = self._trim_codes(event_df, trim_func)
        target_df = self._trim_codes(target_df, trim_func)

        target_df = target_df.groupby(
            ['medical_record_number'] + [
                x for x in target_df.columns if 'context' in x
            ]
        )['age_in_days'].agg(['min', 'max']).reset_index()
        target_df.columns = (
            'medical_record_number', 'target_context', 'target_code',
            'min_age', 'max_age'
        )
        target_df = target_df.drop_duplicates()

        merged = event_df.merge(
            target_df, how='left', on='medical_record_number')
        merged['occurs_after_days'] = merged.min_age - merged.age_in_days

        return merged

    @staticmethod
    def _trim_codes(df, trim_func):
        for code_column in (x for x in df.columns if '_code' in x):
            df[code_column] = df[code_column].apply(
                trim_func
            )
        return df.drop_duplicates()

    @property
    def lab_results(self):
        if self._lab_results is None:
            self._lab_results = read_with_progress("""
                SELECT
                    MEDICAL_RECORD_NUMBER, AGE_IN_DAYS, TEST_CODE, TEST_NAME,
                    LAB_STATUS, RESULT_STATUS, RESULT_FLAG, ABNORMAL_FLAG,
                    TEST_RESULT_VALUE, UNIT_OF_MEASUREMENT
                FROM EPIC_LAB
                WHERE MEDICAL_RECORD_NUMBER IN
                (""" + (", ").join(self.mrns()) + """)
                LIMIT 100000;""", _mysql_engine
            )
            self._lab_results['VALUE'] = pd.to_numeric(
                self._lab_results.TEST_RESULT_VALUE, errors='coerce')
            self._lab_results.dropna(inplace=True)
        return self._lab_results

    def lab_results_for(self, search):
        return self.lab_results[
            self.lab_results.TEST_NAME.str.contains(search)].copy()

    def __len__(self):
        return len(self.mrns())

    def __iter__(self):
        return iter(self.mrns())
