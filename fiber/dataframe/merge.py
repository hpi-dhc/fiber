from functools import reduce
from typing import List, Optional

import pandas as pd

from fiber import OCCURRENCE_INDEX
from fiber.condition.base import _BaseCondition


def merge_event_dfs(
    event_df: pd.DataFrame,
    target_df: pd.DataFrame,
    before: Optional[_BaseCondition] = None,
    after: Optional[_BaseCondition] = None,
):
    """
    Merges two dataframes of condition occurrences based on
    'medical_record_number'. Additionally, can calculate the time delta between
    occurrences and keep only positive/negative ones.

    Args:
        event_df: baseline of the left-outer merge
        target_df: the df to merge with and to compute time_delta_in_days
        before: bool, to remove positive time_delta_in_days
        after: bool, to remove negative time_delta_in_days

    Returns:
        merged df, time-trimmed if specified
    """
    df = event_df.merge(
        target_df, how='left', on='medical_record_number')

    df['time_delta_in_days'] = df.age_in_days_y - df.age_in_days_x
    df.rename(columns={'age_in_days_x': 'age_in_days'}, inplace=True)
    del df['age_in_days_y']

    if after:
        df = df[df.time_delta_in_days >= 0]
    elif before:
        df = df[df.time_delta_in_days <= 0]
    return df


def merge_to_base(
    base: pd.DataFrame,
    dataframes: List[pd.DataFrame]
):
    """
    Merges multiple DataFrames to a base DataFrame along the OCCURRENCE_INDEX.
    This can e.g. be used for combining cohort condition occurrences with
    results from multiple tests.

    Args:
        base: dataframe to merge with in occurrence format
        dataframes: list of result dataframes in occurrence format

    Returns:
        data merged in one single df based on occurrence index
    """
    # (TODO): Make more efficient: https://stackoverflow.com/a/50923275
    checked_frames = [df for df in dataframes if not df.empty]
    merged = reduce(
        lambda left, right: pd.merge(
            left,
            right,
            on=OCCURRENCE_INDEX,
            how='left'
        ),
        [base] + list(checked_frames)
    )
    return merged
