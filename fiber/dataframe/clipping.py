from typing import List, Optional

import pandas as pd


def time_window_clip(
    df: pd.DataFrame,
    window: List
):
    """
    Perform inplace interval-based time-clipping.
    Keep only values within the interval.

    Args:
        df: DataFrame to restrict
        window: inclusive integer interval boundaries
    """
    start, end = window
    return df[
        (df.time_delta_in_days >= start) &
        (df.time_delta_in_days <= end)
    ]


def column_threshold_clip(
    df: pd.DataFrame,
    threshold: Optional[float] = 0
):
    """
    Inplace keep only columns with non-NA values percentage above threshold.

    Args:
        df: DataFrame with NA values in columns
        threshold: can be any float value between: [0.0 - 1.0],
            e.g. 0.7: at least 70% of values in columns do not contain NAN

    Returns:
        df with columns filled above threshold
    """
    return df.loc[:, (df.count() >= (len(df.index) * threshold)).tolist()]
