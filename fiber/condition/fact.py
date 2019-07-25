import yaml
from functools import reduce

import pandas as pd
from sqlalchemy import (
    orm,
    sql,
)
from sqlalchemy.ext.serializer import dumps

from fiber import DEFAULT_STORE_FILE_PATH
from fiber.condition import DatabaseCondition, BaseCondition
from fiber.condition.database import _case_insensitive_like
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
    b_mat,
    fd_diag,
    b_diag,
    fd_proc,
    b_proc,
    d_uom,
)


class FactCondition(DatabaseCondition):

    base_table = fact
    dimensions_map = {
        'PROCEDURE': (fd_proc, b_proc),
        'MATERIAL': (fd_mat, b_mat),
        'DIAGNOSIS': (fd_diag, b_diag),
        'UNIT_OF_MEASURE': (d_uom, 'uom_key', fact, 'uom_key'),
    }
    mrn_column = d_pers.MEDICAL_RECORD_NUMBER

    def __init__(
        self,
        # field: str = '',
        code: str = '',
        context: str = '',
        category: str = '',
        description: str = '',
        **kwargs
    ):
        if 'dimensions' not in kwargs:
            kwargs['dimensions'] = self.dimensions
        super().__init__(**kwargs)
        self.context = context
        self.category = category
        self.code = code
        self.description = description
        self.with_clauses = []

    @property
    def d_table(self):
        """This property should return the dimension table."""
        raise NotImplementedError

    @property
    def code_column(self):
        """"""
        raise NotImplementedError

    @property
    def description_column(self):
        """"""
        raise NotImplementedError

    @property
    def category_column(self):
        """"""
        raise NotImplementedError

    def __getstate__(self):
        if self.children:
            return BaseCondition.__getstate__(self)
        else:
            return {
                'class': self.__class__.__name__,
                'attributes': {
                    'context': self.context,
                    'category': self.category,
                    'code': self.code,
                    'description': self.description,
                },
                'with': [str(dumps(c)) for c in self.with_clauses],
            }

    def create_clause(self):
        clause = sql.true()
        if self.context:
            clause &= self.d_table.CONTEXT_NAME.like(self.context)
        if self.category:
            clause &= _case_insensitive_like(
                self.category_column, self.category)
        if self.code:
            clause &= self.code_column.like(self.code)
        if self.description:
            clause &= _case_insensitive_like(
                self.description_column, self.description)
        for c in self.with_clauses:
            clause &= c
        return clause

    def create_query(self):
        q = orm.Query(self.base_table).join(
            d_pers,
            self.base_table.person_key == d_pers.person_key
        ).with_entities(
            d_pers.MEDICAL_RECORD_NUMBER
        ).distinct()

        q = q.filter(self.clause)

        for dim_name in self.dimensions:
            join_definition = self.dimensions_map[dim_name]
            if len(join_definition) == 2:
                d_table, b_table = join_definition
                d_key = f'{dim_name}_key'
                b_key = f'{dim_name}_group_key'

                q = q.join(
                    b_table,
                    getattr(fact, b_key) == getattr(b_table, b_key)
                ).join(
                    d_table,
                    getattr(d_table, d_key) == getattr(b_table, d_key)
                )
            else:
                join_table, join_key, table, key = join_definition
                q = q.join(
                    join_table,
                    getattr(table, key) == getattr(join_table, join_key)
                )
        return q

    def with_(self, add_clause):
        self.with_clauses.append(add_clause)
        return self


class Procedure(FactCondition):

    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code_column = fd_proc.CONTEXT_PROCEDURE_CODE
    category_column = fd_proc.PROCEDURE_TYPE
    description_column = fd_proc.PROCEDURE_DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]


class Diagnosis(FactCondition):

    dimensions = {'DIAGNOSIS'}
    d_table = fd_diag
    code_column = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category_column = fd_diag.DIAGNOSIS_TYPE
    description_column = fd_diag.DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]

    @classmethod
    def from_condition_store(
        cls,
        name,
        file_path=DEFAULT_STORE_FILE_PATH
    ):
        with open(file_path, 'r') as f:
            diagnosis = yaml.load(f, Loader=yaml.FullLoader)["diagnosis"]
            df = pd.DataFrame.from_dict(diagnosis)

        icd10_codes = reduce(
            cls.__or__,
            [
                cls(context='ICD-10', code=code)
                for code in df[df.name == name]["icd10cm"].iloc[0]
            ]
        )

        icd9_codes = reduce(
            cls.__or__,
            [
                cls(context='ICD-9', code=code)
                for code in df[df.name == name]["icd9cm"].iloc[0]
            ]
        )

        condition = (icd9_codes | icd10_codes)
        condition._label = name
        return condition


class Material(FactCondition):

    dimensions = {'MATERIAL'}
    d_table = fd_mat
    code_column = fd_mat.CONTEXT_MATERIAL_CODE
    category_column = fd_mat.MATERIAL_TYPE
    description_column = fd_mat.MATERIAL_NAME

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]


class VitalSign(Procedure):
    dimensions = {'PROCEDURE', 'UNIT_OF_MEASURE'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        fact.TIME_OF_DAY_KEY,
        fd_proc.CONTEXT_NAME,
        fd_proc.CONTEXT_PROCEDURE_CODE,
        fact.VALUE,
        d_uom.UNIT_OF_MEASURE
    ]

    def __init__(self, description: str = '', **kwargs):
        kwargs['category'] = 'Vital Signs'
        kwargs['description'] = description
        super().__init__(**kwargs)
        self.condition_operation = None
        self.condition_value = None

    def create_clause(self):
        clause = super().create_clause()
        if self.condition_operation:
            clause &= getattr(fact.VALUE, self.condition_operation)(
                              self.condition_value)
        return clause

    # Defining __eq__ removes __hash__  because the hashes have to be equal
    def __hash__(self):
        """Returns a unique hash for the condition definition."""
        return super().__hash__()

    def __getstate__(self):
        json = super().__getstate__()
        json['condition'] = {
            'operation': self.condition_operation,
            'value': self.condition_value,
        }
        return json

    def __lt__(self, other):
        self.condition_operation = '__lt__'
        self.condition_value = other
        return self

    def __le__(self, other):
        self.condition_operation = '__le__'
        self.condition_value = other
        return self

    def __eq__(self, other):
        self.condition_operation = '__eq__'
        self.condition_value = other
        return self

    def __ne__(self, other):
        self.condition_operation = '__ne__'
        self.condition_value = other
        return self

    def __gt__(self, other):
        self.condition_operation = '__gt__'
        self.condition_value = other
        return self

    def __ge__(self, other):
        self.condition_operation = '__ge__'
        self.condition_value = other
        return self


class Drug(Material):

    def __init__(self, name: str = '', *args, **kwargs):
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self.name = name

    def create_clause(self):
        clause = super().create_clause()
        if self.name:
            clause &= (
                fd_mat.MATERIAL_NAME.like(self.name) |
                fd_mat.GENERIC_NAME.like(self.name) |
                fd_mat.BRAND1.like(self.name) |
                fd_mat.BRAND2.like(self.name)
            )
        return clause

    def __getstate__(self):
        json = super().__getstate__()
        json['attributes']['name'] = self.name
        return json
