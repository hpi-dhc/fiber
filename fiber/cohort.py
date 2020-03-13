import json
import math
import time
from collections import defaultdict
from functools import reduce
from typing import List, Optional, Set, Tuple, Union

import pandas as pd

import fiber
from fiber.condition import (
    _DatabaseCondition,
    MRNs,
    Patient,
)
from fiber.condition.base import _BaseCondition
from fiber.config import OCCURRENCE_INDEX
from fiber.dataframe import (
    aggregate_df_with_windows,
    column_threshold_clip,
    create_id_column,
    merge_event_dfs,
    merge_to_base,
    time_window_clip
)
from fiber.extensions import DEFAULT_PIVOT_CONFIG
from fiber.plots.distributions import (
    bars,
    hist,
)
from fiber.storage.json import dict_to_condition
from fiber.utils import Timer


class Cohort:
    """
    A cohort is, conceptually, the basic building-block of FIBER. When querying
    EHR-DB's one wants to define Cohorts, which are basically groups of
    patients, identified via MRN, that have one or more conditions in common,
    in order to run analysis on or to use these for machine learning. In order
    to build cohorts one has to define conditions for the patients that shall
    be part of the cohort.

    A Cohort can be used to fetch, pivot and postprocess data pertaining to
    all of its members.

    Args:
        condition: The condition that defines Cohort belonging.
        limit: The maximum number of Patients that the Cohort should hold.
    """

    def __init__(
        self,
        condition: _BaseCondition,
        limit: Optional[int] = None,
        excluded_mrns: Optional[set] = set(),
        comment: Optional[str] = '',
        version: Optional[str] = '1',
        createdAt: Optional[float] = time.time(),
        fiberVersion: Optional[str] = None
    ):
        self.condition = condition
        self._excluded_mrns = set(excluded_mrns)
        self._mrn_limit = limit
        self._occurrences = None
        self._mrns = None
        self.comment = comment
        self.version = version
        self.created_at = createdAt
        self.fiber_version = fiberVersion or fiber.__version__

    @property
    def mrns(self) -> Set[str]:
        """Get the MRN of each individual Cohort member."""
        if self._mrns is None:
            self._mrns = self.condition.get_mrns(
                limit=self._mrn_limit
            ) - self._excluded_mrns
        return self._mrns

    @property
    def occurrences(self) -> pd.DataFrame:
        """Get a dataframe of all cohort condition occurrences."""
        if self._occurrences is None:
            self._occurrences = self.get_occurrences(self.condition)
        return self._occurrences

    def exclude(self, mrns: Union[Set[str], List[str]]):
        """Exclude specific MRNs from being part of the Cohort.

        Args:
            mrns: A collection of MRNs that will be excluded from the Cohort.
        """
        if isinstance(mrns, set):
            mrns = list(mrns)
        mrns = set(map(str, mrns))
        self._excluded_mrns = self._excluded_mrns | set(mrns)
        self._mrns = None
        return self

    def pivot_all_for(
        self,
        condition: _BaseCondition,
        pivot_table_kwargs: dict,
        threshold: Optional[float] = 0.5,
        window: Optional[Tuple[int]] = (-math.inf, math.inf),
        flatten_columns: Optional[bool] = True
    ) -> pd.DataFrame:
        """Fetches data, aggregates and pivots while time clipping.

        Fetches data for conditions within time windows. Pivots the fetched
        data based on the provided aggregation functions and removes columns
        that are filled below a threshold.
        Additionally applies column renaming magic ✨.

        Args:
            condition: any condition can be used here
            pivot_table_kwargs: args that should be passed to pd.pivot_table
            threshold: columns must be filled above this threshold
            window: relevant time-window (inclusive interval)
            flatten_columns: should column names be flattened from tuples
        """
        df = self.values_for(condition)

        with Timer('Time clipping'):
            df = time_window_clip(df=df, window=window)

        with Timer('Setting indices'):
            df.set_index(OCCURRENCE_INDEX, inplace=True)

        with Timer('Creating id columns'):
            create_id_column(condition, df)

        with Timer('Pivoting'):
            df = pd.pivot_table(
                data=df,
                index=OCCURRENCE_INDEX,
                values=pivot_table_kwargs['aggfunc'].keys(),
                **pivot_table_kwargs
            )

        with Timer('Column threshold clipping'):
            df = column_threshold_clip(
                df=df,
                threshold=threshold
            )

        if flatten_columns:
            with Timer('Flatten column index'):
                df.columns = [
                    '__'.join(col[1:]).strip()
                    for col in df.columns.values
                ]

        return df

    def get_pivoted_features(
        self,
        pivot_config: Optional[dict] = DEFAULT_PIVOT_CONFIG,
    ):
        """Gets all data as specified in the DEFAULT_PIVOT_CONFIG.

        This enables unsupervised machine learning and removes the need
        to specify sophisticated conditions. It can also help to see which
        data are present in the database.

        Args:
            pivot_config: Mapping of conditions to arguments for
                :meth:`fiber.cohort.Cohort.pivot_all_for`

        """
        results = [
            self.pivot_all_for(cond, **pivot_table_kwargs)
            for (cond, pivot_table_kwargs) in pivot_config.items()
        ]

        with Timer('Merge'):
            return self.merge_patient_data(*results)

    def get(
            self,
            data_condition: _BaseCondition,
            *args: _BaseCondition,
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
            if isinstance(cond, _DatabaseCondition):
                database_cond[cond.base_table].append(cond)
            elif isinstance(cond, MRNs):
                database_cond[hash(cond)].append(cond)

        # Get data per BaseTable
        for c in database_cond.values():
            c = reduce(_DatabaseCondition.__or__, c)

            print(f'Fetching data for {c}')
            data.append(c.get_data(self.mrns, limit=limit))
        return data if len(data) > 1 else data[0]

    def get_occurrences(
        self,
        condition: _BaseCondition,
    ):
        """Receive the occurrences of the specified condition for this cohort.

        Args:
            condition: condition to search for occurrences in data regarding
                this cohort

        Returns:
            df containing the occurrences for the specified cohort with
            respective age_in_days-entries.
        """
        # (TODO) Check if selection of distinct timestamp can be moved to db
        # for DatabaseConditions
        occurrences = self.get(condition)
        return occurrences[OCCURRENCE_INDEX].drop_duplicates()

    def _validate_and_get_event_df(
        self,
        relative_to: Optional[_BaseCondition] = None,
        before: Optional[_BaseCondition] = None,
        after: Optional[_BaseCondition] = None
    ):
        """
        functionality to check if only one of relative_to, before or after is
        specified and to fetch data on basis of the parameters. data will be
        either trimmed in case of the before or after parameter, or simply
        returned completely with respective time-deltas in case of relative_to

        Args:
            relative_to: condition describing data-points per MRNs
            before: condition describing data-points per MRNs
            after: condition describing data-points per MRNs

        Returns:
            occurrences of this cohort on basis of the specified
            [relative_to, before, after] from self.get_occurrences
        """
        arg_count = sum([bool(relative_to), bool(before), bool(after)])
        if arg_count == 0:
            return self.occurrences
        elif arg_count == 1:
            return self.get_occurrences(relative_to or before or after)
        else:
            raise ValueError(
                'Only one of (relative_to, before, after) '
                'can be used at the same time.'
            )

    def occurs(
        self,
        target: _BaseCondition,
        relative_to: Optional[_BaseCondition] = None,
        before: Optional[_BaseCondition] = None,
        after: Optional[_BaseCondition] = None
    ):
        """
        functionality to receive data points, but not the values, for this
        cohort on the basis of the specified target condition

        Args:
            target: condition to check for occurrences in this cohort
            relative_to: condition describing data-points per MRNs
            before: condition describing data-points per MRNs
            after: condition describing data-points per MRNs

        Returns:
            df with mrn and age_in_days for the respective condition
        """
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
        target: _BaseCondition,
        relative_to: Optional[_BaseCondition] = None,
        before: Optional[_BaseCondition] = None,
        after: Optional[_BaseCondition] = None
    ):
        """
        functionality to receive data points, including the values, for this
        cohort on the basis of the specified target condition

        Args:
            target: condition to check for occurrences in this cohort
            relative_to: condition describing data-points per MRNs
            before: condition describing data-points per MRNs
            after: condition describing data-points per MRNs

        Returns:
            df with values, mrn, age_in_days for the respective condition
        """
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
        time_windows: List[Tuple[int]],
        df,
        aggregation_functions,
        name: str = 'interval',
    ):
        """
        functionality used to pivot the df within the specified time_windows on
        basis of the aggregation_functions on the specified name

        Args:
            time_windows: the time_windows the data should be trimmed to
            df: the df that shall be pivoted
            aggregation_functions: how to aggregate specified cols when
                pivoting
            name: the name to pivot data towards

        Returns:
            the pivoted, aggregated, prefixed and time-clipped df
        """
        results = aggregate_df_with_windows(
            time_windows,
            df,
            aggregation_functions=aggregation_functions,
            name=name,
        )

        # merge with all occurrences of the cohort condition again
        return merge_to_base(self.occurrences, results)

    def has_occurrence_in(
        self,
        time_windows: List[Tuple[int]],
        df,
        name: str = 'interval',
    ):
        results = aggregate_df_with_windows(
            time_windows,
            df,
            aggregation_functions={
                'time_delta_in_days': lambda x: bool(len(x))
            },
            name=name,
            prefix_column_names=False,
        )

        return merge_to_base(self.occurrences, results).fillna(value=False)

    def has_onset(
        self,
        name: str,
        condition: _BaseCondition,
        time_windows: Optional[List[Tuple[int]]] = None,
    ):
        """
        functionality to check whether the elements of the cohort have an onset
        of the specified condition in the specified time-windows or from
        ((0, 1), (0, 7), (0, 14), (0, 28))-days after the MRNs met the criteria
        of the base_condition of the cohort

        Args:
            name: name of the condition to check for
            condition: the condition to check for
            time_windows: the time_windows for which the data should tested

        Returns:
            df containing the onset in the relative time-window
        """
        co_occurrence = self.occurs(condition)
        time_windows = time_windows or ((0, 1), (0, 7), (0, 14), (0, 28))
        return self.has_occurrence_in(
            time_windows=time_windows,
            df=co_occurrence,
            name=f'{name}_onset',
        )

    def has_precondition(
        self,
        name: str,
        condition: _BaseCondition,
        time_windows: Optional[List[Tuple[int]]] = None,
    ):
        """
        functionality to check if the elements of the cohort have a
        precondition within the specified time-windows

        Args:
            name: name of the precondition
            condition: condition-element to check for
            time_windows: the time_windows the data should be trimmed to

        Returns:
            df containing bool entries for the precondition per mrn
        """
        co_occurrence = self.occurs(condition)
        time_windows = time_windows or ((-math.inf, 0), )
        return self.has_occurrence_in(
            time_windows=time_windows,
            df=co_occurrence,
            name=f'{name}_precondition',
        )

    def time_series_for(
        self,
        target: _BaseCondition,
        relative_to: Optional[_BaseCondition] = None,
        before: Optional[_BaseCondition] = None,
        after: Optional[_BaseCondition] = None,
        aggregate_value_per_day_func=None,
    ):
        """Extracts time series values for conditions with numeric values."""
        assert len(
            [True for s in target.data_columns if 'numeric_value' in s.lower()]
        )
        df = self.values_for(
            target, relative_to=relative_to, before=before, after=after
        )
        if aggregate_value_per_day_func:
            description_column = target.description_column.name.lower()
            if description_column in df.columns:
                grouper = [description_column]
            else:
                grouper = [
                    target.code_column.name.lower(),
                    target.context_column.name.lower()
                ]
            df = df.groupby([
                'medical_record_number',
                'age_in_days',
                'time_delta_in_days',
                *grouper
            ]).agg({
                'numeric_value': aggregate_value_per_day_func
            }).reset_index()
        df.sort_values(
            by=['medical_record_number', 'age_in_days', 'time_delta_in_days'],
            inplace=True,
        )
        return df

    def merge_patient_data(self, *dataframes):
        """
        functionality to merge the data specified in the df's on the patients
        data-points

        Args:
            dataframes: data to merge the patients data with

        Returns:
            data merged in one single df
        """
        base = self.occurrences.merge(
            self.get(Patient()), on=['medical_record_number']
        )
        return merge_to_base(base, dataframes)

    @property
    def demographics(self):
        """
        Generates basic cohort demographics for patients' age and
        gender distribution, including plots.
        """
        condition_events = self.occurrences
        s_age = condition_events[
            condition_events.age_in_days < 50000
        ].groupby(
            'medical_record_number'
        ).age_in_days.mean().apply(lambda x: x / 365).rename('age')
        age_categorization = {
            "minors": sum(s_age < 18) / len(s_age),
            "adults": sum(s_age >= 18) / len(self),
            "adults (anon. age)": (len(self) - sum(s_age >= 18)) / len(self)
        }
        print('''
            [INFO] Age figure and mean exclude patients with anonymized age.
        ''')

        patients_df = self.get(Patient(map_values=True))
        gender_counts = patients_df.gender.value_counts()

        return {
            'raw': {
                'age': s_age,
                'gender': patients_df.gender,
                'race': patients_df.race,
                'deceased': patients_df.deceased_indicator.rename('deceased')
            },
            'age': {
                'mean': s_age.mean(),
                'std': s_age.std(),
                'figure': hist(s_age),
                'distribution': age_categorization
            },
            'gender': {
                'male': gender_counts.Male / sum(gender_counts),
                'female': gender_counts.Female / sum(gender_counts),
                'figure': bars(patients_df.gender)
            },
            'race': {
                'figure': bars(
                    patients_df.race.str.replace('RaceType.', ''),
                    rotate_labels_by=90
                )
            },
            'mortality': {
                'figure': bars(patients_df.deceased_indicator)
            }
        }

    @property
    def condition_statistics(self):
        mrns = self.occurrences.medical_record_number
        occurrence_count = mrns.value_counts().rename('# occurrences')
        return {
            'raw': occurrence_count,
            'mean_count': occurrence_count.mean(),
            'std_count': occurrence_count.std(),
            'figure': bars(occurrence_count)
        }

    def __len__(self):
        """Amount of MRNs in this cohort """
        return len(self.condition)

    def __iter__(self):
        """Iterator object on basis of the MRNs of this cohort """
        return iter(self.mrns)

    def to_json(
        self,
        path: str,
        comment: Optional[str] = None,
        version: Optional[str] = None,
        created_at: Optional[float] = None
    ):
        cohort = {
            'condition': self.condition.to_dict(),
            'excluded_mrns': list(self._excluded_mrns),
            'limit': self._mrn_limit,
            'comment': comment or self.comment,
            'version': version or self.version,
            'createdAt': self.created_at or created_at,
            'fiberVersion': self.fiber_version
        }
        with open(path, 'w') as fp:
            json.dump(cohort, fp)
        return cohort

    @classmethod
    def from_json(cls, path: str):
        with open(path, 'r') as fp:
            cohort_dict = json.load(fp)
        cohort_dict['condition'] = dict_to_condition(cohort_dict['condition'])
        return cls(**cohort_dict)
