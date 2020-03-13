from typing import List, Tuple

import pandas as pd

from fiber.config import OCCURRENCE_INDEX
from fiber.dataframe.clipping import time_window_clip
from fiber.dataframe.helpers import get_name_for_interval


def aggregate_df_with_windows(
    time_windows: List[Tuple[int]],
    df: pd.DataFrame,
    aggregation_functions: dict,
    name: str,
    prefix_column_names: bool = True,
):
    """
    Generalized wrapper for time-window based groupby-agg operations
    on DataFrames with occurrence format.
    This can for example be used to aggregate lab values over time-intervals
    or to find out whether specific onsets happened within weeks or months
    after another condition.

    Args:
        time_windows: List of tuples with integer intervals
        df: DataFrame to operate on
        aggregation_functions: dict of valid aggregation functions,
            for example identified by string, imported from numpy
            or self-written
        name: string to identify result
        prefix_column_names: should prefix column name with aggfunc keys

    Returns:
        data merged in one single df based on occurrence index
    """
    assert all(col in df.columns for col in OCCURRENCE_INDEX), \
        f'DataFrame columns must include {OCCURRENCE_INDEX}'

    results = []

    for window in time_windows:
        window_values = time_window_clip(df, window)

        aggregated_values = window_values.groupby(OCCURRENCE_INDEX).agg(
            aggregation_functions).reset_index()

        # Rename columns
        interval_name = get_name_for_interval(name, window)
        for col_name in aggregation_functions.keys():
            aggregated_values.rename(columns={
                col_name: (
                    f'{col_name}_{interval_name}'
                    if prefix_column_names else interval_name
                )
            }, inplace=True)

        results.append(aggregated_values)

    return results
