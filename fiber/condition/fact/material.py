from fiber.condition.database import _case_insensitive_like
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

    def __init__(self, name: str = '', *args, **kwargs):
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self.name = name

    def create_clause(self):
        clause = super().create_clause()
        if self.name:
            clause &= (
                _case_insensitive_like(fd_mat.MATERIAL_NAME, self.name) |
                _case_insensitive_like(fd_mat.GENERIC_NAME, self.name) |
                _case_insensitive_like(fd_mat.BRAND1, self.name) |
                _case_insensitive_like(fd_mat.BRAND2, self.name)
            )
        return clause

    def to_json(self):
        json = super().to_json()
        # Condition is connected with AND/OR
        if not self.children:
            json['attributes']['name'] = self.name
        return json
