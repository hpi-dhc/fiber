import json


import fiber.condition
from fiber.condition import _BaseCondition


_OPERATORS = {
    _BaseCondition.AND: '__and__',
    _BaseCondition.OR: '__or__',
}


def _get_valid_key(cond_dict):
    """
    functionality to return a key used in the json
    :param cond_dict: the dictionary to get the key from
    :return: key from the dictionary if existing
    """
    for key in cond_dict.keys():
        if key in ('class', *_OPERATORS.keys()):
            return key
    else:
        raise ValueError(f'Incorrect JSON: {cond_dict}')


def dict_to_condition(cond_dict):
    """
    map a json-representation of a condition back to it's python representation
    :param cond_dict: the dictionary to map back
    :return: the condition-object build from the json-dict
    """
    key = _get_valid_key(cond_dict)

    if key == 'class':
        condition_class = getattr(fiber.condition, cond_dict[key])
        return condition_class.from_dict(cond_dict)
    else:
        children = [
            dict_to_condition(child_json)
            for child_json in cond_dict[key]
        ]

        # Reduce child conditions with operator
        condition, *rest = children
        for child in rest:
            condition = getattr(condition, _OPERATORS[key])(child)

        return condition


def dumps(condition):
    """
    dump the condition specified into a json-file
    :param condition: condition to dump to a json-file
    :return: Return a JSON string representation of a Python data structure.
    """
    return json.dumps(condition.to_dict())


def dump(file_pointer, condition):
    """
    dump the condition specified into a json-file specified at file-pointer
    :param file_pointer: the file to dump the json-condition to
    :param condition: condition to dump to a json-file
    :return: Return a JSON string representation of a Python data structure.
    """
    return json.dump(file_pointer, condition.to_dict)


def load(file_pointer):
    """
    load a json-stored condition at a file-pointer-location

    Args:
        file_pointer: the file to load the json-condition from

    :return: Return a JSON string representation of a Python data structure.
    """
    return dict_to_condition(json.load(file_pointer))


def loads(json_string):
    """
    load a json-stored condition at a json_string-location

    Args:
        json_string: the json to load the json-condition from

    :return: Return a JSON string representation of a Python data structure.
    """
    return dict_to_condition(json.loads(json_string))
