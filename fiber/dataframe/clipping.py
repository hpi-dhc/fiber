def time_window_clip(df, window):
    """
    Perform inplace interval-based time-clipping.
    Keep only values within the interval.

    :param df: DataFrame to restrict
    :param window: inclusive integer interval boundaries
    """
    start, end = window
    return df[
        (df.time_delta_in_days >= start) &
        (df.time_delta_in_days <= end)
    ]


def column_threshold_clip(df, threshold=0):
    """

    Inplace keep only columns with non-NA values percentage above threshold.

    :param df: DataFrame with NA values in columns
    :param threshold: can be any float value between: [0.0 - 1.0],
        e.g. 0.7: at least 70% of values in columns do not contain NAN
    :return: df with columns filled above threshold
    """
    return df.loc[:, (df.count() >= (len(df.index) * threshold)).tolist()]
