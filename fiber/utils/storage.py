import json
import fiber.condition
from fiber.condition import BaseCondition


operators = {
    BaseCondition.AND: '__and__',
    BaseCondition.OR: '__or__',
}


def store_json(json_file, condition):
    json_str = json.dumps(condition.to_json())
    with open(json_file, "w") as file:
        file.write(json_str)
    assert load_json(json_file).to_json() == condition.to_json()


def load_json(json_file):
    with open(json_file, "r", encoding='utf-8') as file:
        condition_json = json.load(file)
    return json_to_condition(condition_json)


def json_to_condition(json):
    keys = list(json.keys())

    for operator in operators.keys():
        if operator in keys:
            children = [
                json_to_condition(child_json)
                for child_json in json[operator]
            ]
            condition, *rest = children
            for child in rest:
                condition = getattr(condition, operators[operator])(child)

            return condition
    if 'class' in keys:
        condition_class = getattr(fiber.condition, json['class'])
        return condition_class().from_json(json)
    else:
        raise Exception(f'Incorrect JSON: {json}')
