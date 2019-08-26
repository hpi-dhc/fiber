from sqlalchemy import orm

from fiber.condition import DatabaseCondition
from fiber.condition.mixins import AgeMixin
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
    d_enc,
    d_meta,
)


class FactCondition(AgeMixin, DatabaseCondition):

    base_table = fact
    dimensions_map = {
        'PROCEDURE': (fd_proc, b_proc),
        'MATERIAL': (fd_mat, b_mat),
        'DIAGNOSIS': (fd_diag, b_diag),
        'UNIT_OF_MEASURE': (d_uom, 'uom_key', fact, 'uom_key'),
        'ENCOUNTER': (d_enc, 'encounter_key', fact, 'encounter_key'),
        'METADATA': (d_meta, 'meta_data_key', fact, 'meta_data_key'),
    }
    mrn_column = d_pers.MEDICAL_RECORD_NUMBER

    def __init__(
        self,
        code: str = '',
        context: str = '',
        category: str = '',
        description: str = '',
        **kwargs
    ):
        if 'dimensions' not in kwargs:
            kwargs['dimensions'] = self.dimensions
        super().__init__(**kwargs)
        if bool(code) ^ bool(context):
            raise Exception(
                f"{self.__class__.__name__} code or context missing. Example:"
                f"{self.__class__.__name__}('035.%','ICD-9')")
        self._attrs['code'] = code
        self._attrs['context'] = context
        self._attrs['category'] = category
        self._attrs['description'] = description

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

    @property
    def default_aggregations(self):
        return {
            self.code_column.name.lower(): 'count'
        }

    def create_clause(self):
        clause = super().create_clause()

        if self._attrs['context']:
            clause &= self.d_table.CONTEXT_NAME.like(
                self._attrs['context'])

        if self._attrs['category']:
            clause &= _case_insensitive_like(
                self.category_column, self._attrs['category'])

        if self._attrs['code']:
            clause &= self.code_column.like(self._attrs['code'])

        if self._attrs['description']:
            clause &= _case_insensitive_like(
                self.description_column, self._attrs['description'])

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
