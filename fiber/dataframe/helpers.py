from typing import List, Tuple, Union


def get_name_for_interval(name: str, time_interval: Union[List, Tuple]):
    """
    Helper function to create column names from prefix and time interval,

    Example:
        get_name_for_interval(name='aki', time_interval=[-3, 0])
        -> 'aki_from_3_days_before_to_0_days_after'
    """
    start, end = [
        f'{abs(x)}_days_before' if x < 0 else f'{x}_days_after'
        for x in time_interval
    ]
    name = name.lower().replace(' ', '_')
    return f'{name}_from_{start}_to_{end}'


def create_id_column(condition, df):
    """
    Helper function to create combined column name from taxonomy name and code.
    Additionally, also works with only the description column.

    Example:
        - condition: Diagnosis
        - df with columns:
            - context_name: 'ICD-9'
            - code: '584.9'
    -> Diagnosis__ICD-9__584.9
    """
    if all(k in condition._attrs.keys() for k in ['code', 'context']):
        if not condition._attrs['description']:
            code_column = condition.code_column.name.lower()
            context_column = condition.context_column.name.lower()

            df['description'] = (
                condition.__class__.__name__
                + '__' + df[context_column]
                + '__' + df[code_column]
            )
            del df[context_column]
            del df[code_column]
            return

    description_column = condition.description_column.name.lower()
    df['description'] = [
        f'{condition.__class__.__name__}__{g}'
        for g in df[description_column]
    ]
    del df[description_column]
