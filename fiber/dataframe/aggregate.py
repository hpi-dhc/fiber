from fiber import OCCURRENCE_INDEX
from fiber.dataframe import (
    get_name_for_interval,
    time_window_clip
)


def aggregate_df_with_windows(
    time_windows,
    df,
    aggregation_functions,
    name,
    prefix_column_names=True,
):
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
