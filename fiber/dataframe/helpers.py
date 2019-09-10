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


def create_id_column(cond_cls, df):
    """
    Helper function to create combined column name from taxonomy name and code.

    Example:
        - cond_cls: Diagnosis
        - df with columns:
            - context_name: 'ICD-9'
            - code: '584.9'
    -> Diagnosis__ICD-9__584.9
    """
    code_column = [x for x in df.columns if x.endswith('_code')][0]
    df['code'] = (
        cond_cls.__class__.__name__
        + '__' + df['context_name']
        + '__' + df[code_column]
    ).astype('category')
    del df['context_name']
    del df[code_column]
