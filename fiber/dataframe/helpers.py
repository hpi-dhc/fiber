def get_name_for_interval(name, time_interval):
    start, end = [
        f'{abs(x)}_days_before' if x < 0 else f'{x}_days_after'
        for x in time_interval
    ]
    name = name.lower().replace(' ', '_')
    return f'{name}_from_{start}_to_{end}'


def create_id_column(cond_cls, df):
    code_column = [x for x in df.columns if x.endswith('_code')][0]
    df['code'] = (
        cond_cls.__class__.__name__
        + '__' + df['context_name']
        + '__' + df[code_column]
    ).astype('category')
    del df['context_name']
    del df[code_column]
