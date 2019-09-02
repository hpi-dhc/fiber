from fiber.condition.database import _multi_like_clause
from fiber.condition.fact.fact import FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
)


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


class Drug(Material):

    def __init__(
        self,
        name: str = '',
        brand: str = '',
        generic: str = '',
        *args,
        **kwargs
    ):
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self._attrs['name'] = name
        self._attrs['brand'] = brand
        self._attrs['generic'] = generic

    @property
    def name(self):
        return self._attrs['name']

    def _create_clause(self):
        clause = super()._create_clause()
        if self.name:
            clause &= (
                _multi_like_clause(fd_mat.MATERIAL_NAME, self.name) |
                _multi_like_clause(fd_mat.GENERIC_NAME, self.name) |
                _multi_like_clause(fd_mat.BRAND1, self.name) |
                _multi_like_clause(fd_mat.BRAND2, self.name)
            )
        if self._attrs['brand']:
            clause &= (
                _multi_like_clause(fd_mat.BRAND1, self._attrs['brand']) |
                _multi_like_clause(fd_mat.BRAND2, self._attrs['brand'])
            )
        if self._attrs['generic']:
            clause &= _multi_like_clause(
                fd_mat.GENERIC_NAME, self._attrs['generic'])

        return clause
