def _name_for_interval(name, time_interval):
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