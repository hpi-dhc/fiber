from functools import reduce
import json

import pandas as pd
import yaml

from fiber import DEFAULT_STORE_FILE_PATH
import fiber.condition
from fiber.condition import BaseCondition


operators = {
    BaseCondition.AND: '__and__',
    BaseCondition.OR: '__or__',
}


class JSONStore:
    @staticmethod
    def store_json(json_file, condition):
        json_str = json.dumps(condition.to_json())
        with open(json_file, 'w') as file:
            file.write(json_str)
        assert JSONStore.load_json(json_file).to_json() == condition.to_json()

    @staticmethod
    def load_json(json_file):
        with open(json_file, 'r', encoding='utf-8') as file:
            condition_json = json.load(file)
        return JSONStore.json_to_condition(condition_json)

    @staticmethod
    def json_to_condition(json):
        keys = list(json.keys())

        for operator in operators.keys():
            if operator in keys:
                children = [
                    JSONStore.json_to_condition(child_json)
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


class YAMLStore:
    @staticmethod
    def _open_store(cls, file_path):
        with open(file_path, 'r') as f:
            definitions = yaml.load(f, Loader=yaml.FullLoader)[cls.__name__]
            df = pd.DataFrame.from_dict(definitions)
        return df

    @staticmethod
    def get_available_conditions(
        cls,
        file_path=DEFAULT_STORE_FILE_PATH
    ):
        df = YAMLStore._open_store(cls, file_path)
        return list(df.name)

    @staticmethod
    def get_condition(
        cls,
        name,
        coding_schemes,
        file_path=DEFAULT_STORE_FILE_PATH,
    ):
        df = YAMLStore._open_store(cls, file_path)
        if not df[df.name == name].any().any():
            raise KeyError(name)

        conditions = []
        for context in coding_schemes:
            conditions.append(reduce(
                cls.__or__,
                [
                    cls(context=context, code=code)
                    for code in df[df.name == name][context].iloc[0]
                ]
            ))

        condition = reduce(cls.__or__, [c for c in conditions])
        condition._label = name
        return condition
