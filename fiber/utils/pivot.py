def _name_for_interval(name, time_interval):
    """
    functionality to create names for a specified time-interval

    Args:
        name: the name to combine with the time-interval
        time_interval: the time_interval to combine with the name

    :return: lowercase name built of name, time_interval and before/after
    """
    start, end = [
        f'{abs(x)}_before' if x < 0 else f'{x}_after'
        for x in time_interval
    ]
    name = name.lower().replace(' ', '_')
    return f'{name}_{start}_{end}'


def pivot_df_with_windows(
    time_windows,
    df,
    aggregation_functions,
    name,
    prefix_column_names=True,
):
    """
    functionality to pivot the df with the specified aggregation functions on
    the basis of the name, trimmed to the specified time_windows and prefixed
    if prefix_column_names set to TRUE.

    Args:
        time_windows: the time_windows the pivoted data should be trimmed to
        df: the df that shall be pivoted
        aggregation_functions: how to aggregate specified cols when pivoting
        name: the name to pivot data towards
        prefix_column_names: either [True, False], depending on if a prefix
            to the newly created pivot_columns shall be created

    :return: the pivoted, aggregated, prefixed and time-clipped df
    """
    grouping_columns = ['medical_record_number', 'age_in_days']
    assert all(col in df.columns for col in grouping_columns), \
        f'DataFrame columns must include {grouping_columns}'

    results = []

    for window in time_windows:
        start, end = window
        window_values = df[
            (df.time_delta_in_days >= start) &
            (df.time_delta_in_days <= end)
        ]

        aggregated_values = window_values.groupby(grouping_columns).agg(
            aggregation_functions).reset_index()

        # Rename columns
        interval_name = _name_for_interval(name, window)
        for col_name in aggregation_functions.keys():
            aggregated_values.rename(columns={
                col_name: (
                    f'{col_name}_{interval_name}'
                    if prefix_column_names else interval_name
                )
            }, inplace=True)

        results.append(aggregated_values)

    return results
