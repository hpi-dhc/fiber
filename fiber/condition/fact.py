import yaml
import pandas as pd
from sqlalchemy import orm
from functools import reduce

from fiber import DEFAULT_STORE_FILE_PATH
from fiber.condition import DatabaseCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
    b_mat,
    fd_diag,
    b_diag,
    fd_proc,
    b_proc,
)


def _case_insensitive_like(column, value):
    return column.like(value)
    # Actual case insensitivity causes OOM at the moment.
    # return func.upper(column).like(value.upper())


class FactCondition(DatabaseCondition):

    base_table = fact
    dimensions_map = {
        'PROCEDURE': (fd_proc, b_proc),
        'MATERIAL': (fd_mat, b_mat),
        'DIAGNOSIS': (fd_diag, b_diag),
    }

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

        # (TODO) Think about == Syntax vs Constructor args.
        # if field:
        #     self._column =
        # else:
        if context:
            self.clause &= _case_insensitive_like(
                self.d_table.CONTEXT_NAME, context)
        if category:
            self.clause &= _case_insensitive_like(
                self.category, category)
        if code:
            self.clause &= _case_insensitive_like(
                self.code, code)
        if description:
            self.clause &= _case_insensitive_like(
                self.description, description)

    @property
    def d_table(self):
        """This property should return the dimension table."""
        raise NotImplementedError

    @property
    def code(self):
        """"""
        raise NotImplementedError

    @property
    def description(self):
        """"""
        raise NotImplementedError

    def create_query(self):
        q = orm.Query(self.base_table).join(
            d_pers,
            self.base_table.person_key == d_pers.person_key
        ).with_entities(
                d_pers.MEDICAL_RECORD_NUMBER
        ).distinct()

        q = q.filter(self.clause)

        for dim_name in self.dimensions:
            d_table, b_table = self.dimensions_map[dim_name]
            d_key = f'{dim_name}_key'
            b_key = f'{dim_name}_group_key'

            q = q.join(
                b_table,
                getattr(fact, b_key) == getattr(b_table, b_key)
            ).join(
                d_table,
                getattr(d_table, d_key) == getattr(b_table, d_key)
            )
        return q

    def mrn_filter(self, mrns):
        return d_pers.MEDICAL_RECORD_NUMBER.in_(mrns)

    def with_(self, add_clause):
        self.clause &= add_clause
        return self


class Procedure(FactCondition):

    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code = fd_proc.CONTEXT_PROCEDURE_CODE
    category = fd_proc.PROCEDURE_TYPE
    description = fd_proc.PROCEDURE_DESCRIPTION

    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code
    }


class Diagnosis(FactCondition):

    dimensions = {'DIAGNOSIS'}
    d_table = fd_diag
    code = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category = fd_diag.DIAGNOSIS_TYPE
    description = fd_diag.DESCRIPTION

    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code
    }

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

        return icd9_codes | icd10_codes


class Material(FactCondition):

    dimensions = {'MATERIAL'}
    d_table = fd_mat
    code = fd_mat.CONTEXT_MATERIAL_CODE
    category = fd_mat.MATERIAL_TYPE
    description = fd_mat.MATERIAL_NAME

    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code
    }


class VitalSign(Procedure):

    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        fact.TIME_OF_DAY_KEY,
        fd_proc.CONTEXT_NAME,
        fd_proc.CONTEXT_PROCEDURE_CODE,
        fact.VALUE
    }

    def __init__(self, description, **kwargs):
        kwargs['category'] = 'Vital Signs'
        kwargs['description'] = description
        super().__init__(**kwargs)

    def __lt__(self, other):
        self.clause &= getattr(fact.VALUE, '__lt__')(other)
        return self

    def __le__(self, other):
        self.clause &= getattr(fact.VALUE, '__le__')(other)
        return self

    def __eq__(self, other):
        self.clause &= getattr(fact.VALUE, '__eq__')(other)
        return self

    def __ne__(self, other):
        self.clause &= getattr(fact.VALUE, '__ne__')(other)
        return self

    def __gt__(self, other):
        self.clause &= getattr(fact.VALUE, '__gt__')(other)
        return self

    def __ge__(self, other):
        self.clause &= getattr(fact.VALUE, '__ge__')(other)
        return self


class Drug(Material):

    def __init__(self, name: str = '', *args, **kwargs):
        kwargs['category'] = 'Drug'

        super().__init__(*args, **kwargs)

        if name:
            self.clause &= (
                fd_mat.MATERIAL_NAME.like(name) |
                fd_mat.GENERIC_NAME.like(name) |
                fd_mat.BRAND1.like(name) |
                fd_mat.BRAND2.like(name)
            )
