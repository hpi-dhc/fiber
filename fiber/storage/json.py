import json
from io import StringIO

import fiber.condition
from fiber.condition import _BaseCondition


_OPERATORS = {
    _BaseCondition.AND: '__and__',
    _BaseCondition.OR: '__or__',
}


def _get_valid_key(cond_dict: dict):
    """functionality to return a key used in the json

    Args:
        cond_dict: the dictionary to get the key from
    Returns:
        key from the dictionary if existing
    """
    for key in cond_dict.keys():
        if key in ('class', *_OPERATORS.keys()):
            return key
    else:
        raise ValueError(f'Incorrect JSON: {cond_dict}')


def dict_to_condition(cond_dict: dict):
    """Map a json of a condition back to its object representation

    Args:
        cond_dict: the dictionary to map back
    Returns:
        the condition-object build from the json-dict
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


def dumps(condition: _BaseCondition):
    """Dump the condition specified into a json-file
    Args:
        condition: condition to dump to a json-file

    Returns:
        Return a JSON string representation of a Python data structure.
    """
    return json.dumps(condition.to_dict())


def dump(file_pointer: StringIO, condition: _BaseCondition):
    """Dump the condition specified into a json-file specified at file-pointer

    Args:
        file_pointer: the file to dump the json-condition to
        condition: condition to dump to a json-file

    Returns:
        Return a JSON string representation of a Python data structure.
    """
    return json.dump(condition.to_dict(), file_pointer)


def load(file_pointer: StringIO):
    """Load a json-stored condition at a file-pointer-location

    Args:
        file_pointer: the file to load the json-condition from

    Returns:
        Return a JSON string representation of a Python data structure.
    """
    return dict_to_condition(json.load(file_pointer))


def loads(json_string: str):
    """Load a json-stored condition at a json_string-location

    Args:
        json_string: the json to load the json-condition from

    Returns:
        Return a JSON string representation of a Python data structure.
    """
    return dict_to_condition(json.loads(json_string))
