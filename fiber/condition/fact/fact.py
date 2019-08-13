from sqlalchemy import (
    orm,
    sql,
)

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
    d_enc,
    d_meta,
)


class FactCondition(DatabaseCondition):

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
        self.code = code
        self.context = context
        self.category = category
        self.description = description
        self.age_conditions = []

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

    def to_json(self):
        if self.children:
            return BaseCondition.to_json(self)
        else:
            return {
                'class': self.__class__.__name__,
                'attributes': {
                    'context': self.context,
                    'category': self.category,
                    'code': self.code,
                    'description': self.description,
                },
                'age_in_days': self.age_conditions,
            }

    def from_json(self, json):
        condition = super().from_json(json)
        for a in json['age_in_days']:
            condition.age_in_days(a['min_days'], a['max_days'])
        return condition

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
        for a in self.age_conditions:
            if a['min_days']:
                clause &= fact.AGE_IN_DAYS >= a['min_days']
            if a['max_days']:
                clause &= fact.AGE_IN_DAYS < a['max_days']
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

    def age(self, min_age=None, max_age=None):
        self.age_conditions.append(
            {'min_days': 365 * min_age if min_age else None,
             'max_days': 365 * max_age if max_age else None})
        return self

    def age_in_days(self, min_days=None, max_days=None):
        self.age_conditions.append({'min_days': min_days,
                                    'max_days': max_days})
        return self
