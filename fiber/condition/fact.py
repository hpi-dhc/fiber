from sqlalchemy import orm

from fiber.condition import DatabaseCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
    fd_diag,
    fd_proc,
    filter_by,
)


def _case_insensitive_like(column, value):
    return column.like(value)
    # Actual case insensitivity causes OOM at the moment.
    # return func.upper(column).like(value.upper())


class FactCondition(DatabaseCondition):

    base_table = fact

    def create_query(self):
        base = orm.Query(self.base_table).join(
            d_pers,
            self.base_table.person_key == d_pers.person_key
        ).with_entities(
                d_pers.MEDICAL_RECORD_NUMBER
        ).distinct()

        # Join relevant dimensions to base query
        return filter_by(base, self)

    def mrn_filter(self, mrns):
        return d_pers.MEDICAL_RECORD_NUMBER.in_(mrns)

    @property
    def dimension_table(self):
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

    @property
    def _default_columns(self):
        return {
            d_pers.MEDICAL_RECORD_NUMBER,
            fact.AGE_IN_DAYS,
            self.dimension_table.CONTEXT_NAME,
            self.code,
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
                self.dimension_table.CONTEXT_NAME, context)
        if category:
            self.clause &= _case_insensitive_like(
                self.category, category)
        if code:
            self.clause &= _case_insensitive_like(
                self.code, code)
        if description:
            self.clause &= _case_insensitive_like(
                self.description, description)

    def with_(self, add_clause):
        self.clause &= add_clause
        return self


class Procedure(FactCondition):

    dimensions = {'PROCEDURE'}

    dimension_table = fd_proc
    code = fd_proc.CONTEXT_PROCEDURE_CODE
    category = fd_proc.PROCEDURE_TYPE
    description = fd_proc.PROCEDURE_DESCRIPTION


class Diagnosis(FactCondition):

    dimensions = {'DIAGNOSIS'}

    dimension_table = fd_diag
    code = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category = fd_diag.DIAGNOSIS_TYPE
    description = fd_diag.DESCRIPTION


class Material(FactCondition):

    dimensions = {'MATERIAL'}

    dimension_table = fd_mat
    code = fd_mat.CONTEXT_MATERIAL_CODE
    category = fd_mat.MATERIAL_TYPE
    description = fd_mat.MATERIAL_NAME


class VitalSign(Procedure):

    def __init__(self, description, **kwargs):
        kwargs['category'] = 'Vital Signs'
        kwargs['description'] = description
        super().__init__(**kwargs)

    @property
    def _default_columns(self):
        return {
            d_pers.MEDICAL_RECORD_NUMBER,
            fact.AGE_IN_DAYS,
            fact.TIME_OF_DAY_KEY,
            self.dimension_table.CONTEXT_NAME,
            self.code,
            fact.VALUE
        }


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


def make_method(name):
    def _method(self, other):
        self.clause &= getattr(fact.VALUE, name)(other)
        return self
    return _method


for magic_method in (
    '__gt__',
    '__lt__',
    '__le__',
    '__eq__',
    '__ne__',
    '__ge__',
):
    _method = make_method(magic_method)
    setattr(VitalSign, magic_method, _method)
