from functools import reduce
import pandas as pd
import yaml

from fiber import DEFAULT_STORE_FILE_PATH


def _open_store(condition_class, file_path):
    """
    open json-store for condition specified by condition_class at file_path and
    return the df containing the loaded data

    Args:
        condition_class: the condition to load into
        file_path: the path of the json-file

    :return: df containing the loaded data, formerly represented as json
    """
    with open(file_path, 'r') as f:
        definitions = yaml.load(
            f, Loader=yaml.FullLoader)[condition_class.__name__]
        df = pd.DataFrame.from_dict(definitions)
    return df


def get_available_conditions(
    condition_class,
    file_path=DEFAULT_STORE_FILE_PATH
):
    """
    retrieve a list of the conditions stored for the class specified

    Args:
        condition_class: the condition to search for in the specified
            json-file_path store or at DEFAULT_STORE_FILE_PATH
        file_path: the path to load the available stored conditions from

    :return: list of the conditions stored
    """
    df = _open_store(condition_class, file_path)
    return list(df.name)


def get_condition(
    condition_class,
    name,
    coding_schemes,
    file_path=DEFAULT_STORE_FILE_PATH,
):
    """
    receive the condition specified by condition_class, name and coding_scheme
        from the json-file_path specified or the DEFAULT_STORE_FILE_PATH

    Args:
        condition_class: the class of conditions to load
        name: the name of the condition to load
        coding_schemes: context's coding the condition
        file_path: the path of the json-file to search for the condition

    :return: condition as loaded from the json-store
    """
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
