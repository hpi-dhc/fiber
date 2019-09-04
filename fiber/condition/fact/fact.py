from typing import (
    Iterable,
    Union,
)

from sqlalchemy import orm

from fiber.condition import _DatabaseCondition
from fiber.condition.mixins import AgeMixin
from fiber.condition.database import (
    _case_insensitive_like,
    _multi_like_clause,
)
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


class _FactCondition(AgeMixin, _DatabaseCondition):
    """
    The _FactCondition adds functionality to the _DatabaseCondition. It allows
    to combine SQL Statements that shall be performed on the FACT-Table with
    age-constraints on the dates.

    It also defines which dimensions shall be mapped in which fashion, in order
    to capsule the context-joins on db-side from the user.
    """
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
        code: Union[str, Iterable[str]] = '',
        context: str = '',
        category: str = '',
        description: str = '',
        **kwargs
    ):
        """
        Args:
            code: can be any code from the medical context, like 035.%
                the '%' sign works as a *-mapping.
            context: can be any context from the medical context, like ICD-9
            category: the category to search for
            description: can be any string to search for.
                Can contain %-Wildcards.
            kwargs: arguments that shall be passed higher in the hierarchy
        """
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
        """This property should return the code column."""
        raise NotImplementedError

    @property
    def description_column(self):
        """This property should return the description column."""
        raise NotImplementedError

    @property
    def category_column(self):
        """This property should return the category column."""
        raise NotImplementedError

    @property
    def default_aggregations(self):
        """This returns the default-aggregations: 'count' the code-column."""
        return {
            self.code_column.name.lower(): 'count'
        }

    def _create_clause(self):
        """
        Used to create a SQLAlchemy clause based on the fact-condition.
        It is used to select the correct patients.
        """
        clause = super()._create_clause()

        if self._attrs['context']:
            clause &= self.d_table.CONTEXT_NAME.like(
                self._attrs['context'])

        if self._attrs['category']:
            clause &= _case_insensitive_like(
                self.category_column, self._attrs['category'])

        if self._attrs['code']:
            clause &= _multi_like_clause(self.code_column, self._attrs['code'])

        if self._attrs['description']:
            clause &= _case_insensitive_like(
                self.description_column, self._attrs['description'])

        return clause

    def _create_query(self):
        """
        Creates an instance of a SQLAlchemy query which only returns MRNs.

        This query should yield all medical record numbers in the
        ``base_table`` of the condition (fact in this case).
        It uses the ``.clause`` to select the relevant patients.

        This query is also used by other function which change the selected
        columns to get data about the patients.
        """

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
