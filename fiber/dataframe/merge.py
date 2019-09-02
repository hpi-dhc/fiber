import pandas as pd
from functools import reduce

from fiber import OCCURRENCE_INDEX


def merge_event_dfs(
    event_df,
    target_df,
    before=None,
    after=None,
):
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


def merge_to_base(base, dataframes):
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
