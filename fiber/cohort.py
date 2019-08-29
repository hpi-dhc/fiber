import math
from collections import defaultdict
from functools import reduce
from typing import Set, List, Union, Optional

import pandas as pd

from fiber.condition.base import BaseCondition
from fiber.condition import (
    DatabaseCondition,
    MRNs,
    Patient,
)
from fiber.database.table import d_pers
from fiber.plots.distributions import (
    bars,
    hist,
)

from fiber.utils.merge import (
    merge_event_dfs,
    merge_to_base,
)

from fiber.utils.pivot import pivot_df_with_windows


class Cohort:
    """ Patients that share common conditions make up a cohort.

    A Cohort can be used to fetch, pivot and postprocess data pertaining to
    all of its members.

    Args:
        condition: The condition that defines Cohort belonging.
        limit: The maximum number of Patients that the Cohort should hold.
    """

    def __init__(self, condition: BaseCondition, limit: Optional[int] = None):
        self._condition = condition
        self._excluded_mrns = set()
        self._mrn_limit = limit

    def mrns(self) -> Set[str]:
        """Get the MRN of each individual Cohort member."""
        return (self._condition.get_mrns(limit=self._mrn_limit)
                - self._excluded_mrns)

    def exclude(self, mrns: Union[Set[str], List[str]]):
        """ Exclude specific MRNs from being part of the Cohort.

        Args:
            mrns: A collection of MRNs that will be excluded from the Cohort.
        """
        if isinstance(mrns, set):
            mrns = list(mrns)
        mrns = set(map(str, mrns))
        self._excluded_mrns = self._excluded_mrns | set(mrns)
        return self

    def get(
            self,
            data_condition: BaseCondition,
            *args: BaseCondition,
            limit: Optional[int] = None,
    ) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """Fetch data for all members of the Cohort.

        Args:
            data_condition: A condition that describes data points.
            *args: Further data_conditions.
            limit: Limit for the number of returned data points.

        Examples:
            >>> cohort.get(LabValue())
            pd.DataFrame(...)

            >>> cohort.get(LabValue(), Drug(), limit=100)
            [pd.DataFrame(...), pd.DataFrame(...)]

        """
        data_conditions = [data_condition] + list(args)

        data = []
        # Group Data by BaseTable (:/ only works for DatabaseConditions)
        database_cond = defaultdict(list)
        for cond in data_conditions:
            if isinstance(cond, DatabaseCondition):
                database_cond[cond.base_table].append(cond)
            elif isinstance(cond, MRNs):
                database_cond[hash(cond)].append(cond)

        # Get data per BaseTable
        for c in database_cond.values():
            c = reduce(DatabaseCondition.__or__, c)

            print(f'Fetching data for {c}')
            data.append(c.get_data(self.mrns(), limit=limit))
        return data if len(data) > 1 else data[0]

    def get_occurrences(
        self,
        condition,
    ):
        # (TODO) Check if selection of distinct timestamp can be moved to db
        # for DatabaseConditions
        occurrences = self.get(condition)

        return occurrences[
            ['medical_record_number', 'age_in_days']
        ].drop_duplicates()

    def _validate_and_get_event_df(
        self,
        relative_to=None, before=None, after=None,
    ):
        if not bool(relative_to) ^ bool(before) ^ bool(after):
            raise ValueError(
                'Only one of (relative_to, before, after) can be used.'
            )
        else:
            return self.get_occurrences(relative_to or before or after)

    def occurs(
        self,
        target,
        relative_to=None,
        before=None,
        after=None
    ):
        event_df = self._validate_and_get_event_df(
            relative_to, before, after)
        target_df = self.get_occurrences(target)

        df = merge_event_dfs(
            event_df,
            target_df,
            before=before,
            after=after
        )

        return df

    def values_for(
        self,
        target,
        relative_to=None,
        before=None,
        after=None
    ):
        event_df = self._validate_and_get_event_df(
            relative_to, before, after)
        target_df = self.get(target)

        return merge_event_dfs(
            event_df,
            target_df,
            before=before,
            after=after
        )

    def aggregate_values_in(
        self,
        time_windows,
        df,
        aggregation_functions,
        name: str = 'interval',
    ):
        results = pivot_df_with_windows(
            time_windows,
            df,
            aggregation_functions=aggregation_functions,
            name=name,
        )

        # merge with all occurrences of the cohort condition again
        base = self.get_occurrences(self._condition)
        return merge_to_base(base, results)

    def has_occurrence_in(
        self,
        time_windows,
        df,
        name: str = 'interval',
    ):

        results = pivot_df_with_windows(
            time_windows,
            df,
            aggregation_functions={'time_delta_in_days': 'any'},
            name=name,
            prefix_column_names=False,
        )

        # merge with all occurrences of the cohort condition again
        base = self.get_occurrences(self._condition)
        return merge_to_base(base, results).fillna(value=False)

    def has_onset(
        self,
        name,
        condition,
        time_windows=None,
    ):
        co_occurrence = self.occurs(
            condition, relative_to=self._condition
        )

        time_windows = time_windows or ((0, 1), (0, 7), (0, 14), (0, 28))
        return self.has_occurrence_in(
            time_windows=time_windows,
            df=co_occurrence,
            name=f'{name}_onset',
        )

    def has_precondition(
        self,
        name,
        condition,
        time_windows=None,
    ):
        co_occurrence = self.occurs(
            condition, relative_to=self._condition
        )

        time_windows = time_windows or ((-math.inf, 0), )
        return self.has_occurrence_in(
            time_windows=time_windows,
            df=co_occurrence,
            name=f'{name}_precondition',
        )

    def merge_patient_data(self, *dataframes):
        base = self.get_occurrences(self._condition).merge(
            self.get(Patient()), on=['medical_record_number']
        )
        return merge_to_base(base, dataframes)

    @property
    def demographics(self):
        '''
        Generates basic cohort demographics for patients' age and
        gender distribution, including plots.
        '''
        condition_events = self.get_occurrences(self._condition)
        s_age = condition_events[
            condition_events.age_in_days < 50000
        ].groupby(
            ['medical_record_number']
        ).age_in_days.mean().apply(lambda x: x / 365)
        s_age.rename('age', inplace=True)
        print('[INFO] Age demographics exclude patients with anonymized age.')

        s_gender = self.get(
            Patient(data_columns=[d_pers.MEDICAL_RECORD_NUMBER, d_pers.GENDER])
        ).gender

        gender_counts = s_gender.value_counts()
        return {
            'age': {
                'mean': s_age.mean(),
                'std': s_age.std(),
                'figure': hist(s_age)
            },
            'gender': {
                'male': gender_counts.Male / sum(gender_counts),
                'female': gender_counts.Female / sum(gender_counts),
                'figure': bars(s_gender)
            }
        }

    def __len__(self):
        return len(self._condition)

    def __iter__(self):
        return iter(self.mrns())
