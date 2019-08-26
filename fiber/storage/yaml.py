from functools import reduce
import pandas as pd
import yaml

from fiber import DEFAULT_STORE_FILE_PATH


def _open_store(condition_class, file_path):
    with open(file_path, 'r') as f:
        definitions = yaml.load(
            f, Loader=yaml.FullLoader)[condition_class.__name__]
        df = pd.DataFrame.from_dict(definitions)
    return df


def get_available_conditions(
    condition_class,
    file_path=DEFAULT_STORE_FILE_PATH
):
    df = _open_store(condition_class, file_path)
    return list(df.name)


def get_condition(
    condition_class,
    name,
    coding_schemes,
    file_path=DEFAULT_STORE_FILE_PATH,
):
    df = _open_store(condition_class, file_path)
    if not df[df.name == name].any().any():
        raise KeyError(name)

    conditions = []
    for context in coding_schemes:
        conditions.append(reduce(
            condition_class.__or__,
            [
                condition_class(context=context, code=code)
                for code in df[df.name == name][context].iloc[0]
            ]
        ))

    condition = reduce(condition_class.__or__, [c for c in conditions])
    condition._label = name
    return condition
