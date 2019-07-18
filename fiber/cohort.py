from collections import defaultdict
from functools import reduce
from typing import Set, List, Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from fiber.condition.base import BaseCondition
from fiber.condition import (
    DatabaseCondition,
    Patient,
)
from fiber.database.table import d_pers


class Cohort:

    def __init__(self, condition: BaseCondition, limit=None):

        self._condition = condition
        self._lab_results = None
        self._excluded_mrns = set()
        self._limit = limit

    def mrns(self):
        return (self._condition.get_mrns(limit=self._limit)
                - self._excluded_mrns)

    def get(self, *data_conditions, limit=None):
        data = []

        database_cond = defaultdict(list)
        for cond in data_conditions:
            if isinstance(cond, DatabaseCondition):
                database_cond[cond.base_table].append(cond)

        # (TODO) What might be better: Return a list of DataFrames for every
        # data condition and don't merge them:
        #
        # for c in data_conditions:

        for c in database_cond.values():
            c = reduce(DatabaseCondition.__or__, c)

            print(f'Fetching data for {c}..')
            data.append(c.get_data(self.mrns(), limit=limit))
        return data if len(data) > 1 else data[0]

    def exclude(self, mrns: Union[Set[str], Set[int], List[str], List[int]]):
        if isinstance(mrns, set):
            mrns = list(mrns)
        mrns = set(map(str, mrns))
        self._excluded_mrns = self._excluded_mrns | set(mrns)
        return self

    def occurs(
        self,
        target,
        relative_to=None,
        before=None,
        after=None,
        trim_func=lambda x: x.split('.')[0],
    ):
        if not bool(relative_to) ^ bool(before) ^ bool(after):
            raise ValueError(
                'Only one of (relative_to, before, after) can be used.'
            )
        event = relative_to or before or after

        event_df = self.get(event)
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

        df = event_df.merge(
            target_df, how='left', on='medical_record_number')

        if relative_to or after:
            df['occurs_after_x_days'] = df.min_age - df.age_in_days
            if after:
                df = df[df.occurs_after_x_days >= 0]
        else:
            df['occurs_x_days_before'] = df.age_in_days - df.min_age
            df = df[df.occurs_x_days_before >= 0]

        return df

    @staticmethod
    def _trim_codes(df, trim_func):
        for code_column in (x for x in df.columns if '_code' in x):
            df[code_column] = df[code_column].apply(
                trim_func
            )
        return df.drop_duplicates()

    @property
    def demographics(self):
        '''
        Generates basic cohort demographics, such as mean and standard
        deviation for age and the gender distribution - including
        plots.
        '''
        condition_events = self.get(self._condition)
        df_age = condition_events[
            condition_events.age_in_days < 50000
        ].groupby(
            ["medical_record_number"]
        ).age_in_days.mean().apply(lambda x: x / 365)

        df_gender = self.get(
            Patient(data_columns=[d_pers.MEDICAL_RECORD_NUMBER, d_pers.GENDER])
        )
        gender_counts = df_gender.gender.value_counts()

        gender_figure, gender_ax = plt.subplots()
        age_figure, age_ax = plt.subplots()

        sns.distplot(df_age, ax=age_ax)
        sns.countplot(df_gender.gender, palette="RdBu", ax=gender_ax)

        # TODO: DEMOGRAPHICS OBJECT
        return {
            "age": {
                "mean": df_age.mean(),
                "std": df_age.std(),
                "figure": age_figure
            },
            "gender": {
                "distribution": {
                    "male": gender_counts.Male / sum(gender_counts),
                    "female": gender_counts.Female / sum(gender_counts),
                },
                "figure": gender_figure
            }
        }

    def has_onset(self, name, condition, time_deltas=[1, 7, 14, 28]):
        occurrences = self.occurs((condition), after=self._condition)[
            ["medical_record_number", "occurs_after_x_days"]
        ]

        df = pd.DataFrame(self.mrns(), columns=["medical_record_number"])
        for i in time_deltas:
            mrns = set(occurrences[
                occurrences.occurs_after_x_days <= i
            ].medical_record_number)
            df[f"{name}_{i}_days"] = df.medical_record_number.isin(mrns)

        return df

    def has_precondition(self, condition):
        mrns = set(
            self.occurs(
                (condition), before=self._condition
            ).medical_record_number
        )

        df = pd.DataFrame(self.mrns(), columns=["medical_record_number"])
        df[condition._label] = df.medical_record_number.isin(mrns)
        return df

    def results_for(
        self,
        target,
        relative_to=None,
        before=None,
        after=None,
        trim_func=lambda x: x.split('.')[0],
    ):
        if not bool(relative_to) ^ bool(before) ^ bool(after):
            raise ValueError(
                'Only one of (relative_to, before, after) can be used.'
            )
        event = relative_to or before or after

        event_df = self.get(event)
        event_df = self._trim_codes(event_df, trim_func)
        target_df = self.get(target)

        target_df = target_df.groupby(
            ['medical_record_number', 'age_in_days']
        )['numeric_value'].mean().reset_index()

        df = event_df.merge(
            target_df, how='left', on='medical_record_number')

        if relative_to or after:
            df['occurs_after_x_days'] = df.age_in_days_y - df.age_in_days_x
            if after:
                df = df[df.occurs_after_x_days >= 0]
        else:
            df['occurs_x_days_before'] = df.age_in_days_x - df.age_in_days_y
            df = df[df.occurs_x_days_before >= 0]

        df.rename(columns={"age_in_days_x": "age_in_days"})
        return df[[
            "medical_record_number",
            "age_in_days_x",
            "numeric_value",
            "occurs_x_days_before"
        ]]

    def build_data(self, *dataframes):
        base = pd.DataFrame(self, columns=['medical_record_number'])
        patient_data = self.get(Patient())
        merged = reduce(
            lambda l, r: pd.merge(l, r, on='medical_record_number'),
            [base, patient_data] + list(dataframes)
        )
        return merged

    def __len__(self):
        return len(self._condition)

    def __iter__(self):
        return iter(self.mrns())
