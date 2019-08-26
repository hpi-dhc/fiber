import json


import fiber.condition
from fiber.condition import BaseCondition


_OPERATORS = {
    BaseCondition.AND: '__and__',
    BaseCondition.OR: '__or__',
}


def _get_valid_key(cond_dict):
    for key in cond_dict.keys():
        if key in ('class', *_OPERATORS.keys()):
            return key
    else:
        raise ValueError(f'Incorrect JSON: {cond_dict}')


def dict_to_condition(cond_dict):

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
    return json.dumps(condition.to_dict())


def dump(file_pointer, condition):
    return json.dump(file_pointer, condition.to_dict)


def load(file_pointer):
    return dict_to_condition(json.load(file_pointer))


def loads(json_string):
    return dict_to_condition(json.loads(json_string))
