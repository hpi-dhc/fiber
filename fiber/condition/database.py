from typing import Set

from sqlalchemy import (
    func,
    orm,
    sql,
)

import fiber
from fiber.condition.base import BaseCondition
from fiber.database import (
    compile_sqla,
    read_with_progress,
)
from fiber.database.hana import engine
from fiber.database.table import Table


def _case_insensitive_like(column, value):
    # return column.like(value)
    # Actual case insensitivity somewhat memory intensive at the moment.
    return func.upper(column).like(value.upper())


class DatabaseCondition(BaseCondition):

    engine = engine

    def __init__(
        self,
        mrns: Set[str] = None,
        dimensions: Set[str] = None,
        clause=None,
        data_columns=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dimensions = dimensions or set()
        # sql.true() acts as an 'empty' initializer for the clause
        self._clause = sql.true() if clause is None else clause
        self._specified_columns = data_columns or []
        self._data_cache = {}

    @property
    def base_table(self) -> Table:
        """This property should be set to the database table containing data.

        All queries generated from a `DatabaseCondition` are constraints
        on this base table. If the base table matches, conditions can
        potentially be combined into a single query.
        """
        raise NotImplementedError

    @property
    def _default_columns(self):
        """Array of default columns of the database data query"""
        raise NotImplementedError

    @property
    def mrn_column(self):
        """The column of the base table that holds the medical record number"""
        raise NotImplementedError

    @property
    def data_columns(self):
        """Returns the column selection for fetching data points."""
        return self._specified_columns or self._default_columns

    @property
    def clause(self):
        """Returns the clause of the current condition or creates the clause"""
        if not isinstance(self._clause, sql.elements.True_):
            return self._clause
        else:
            return self.create_clause()

    def create_clause(self):
        """Should create a SQLAlchemy clause for the specific condition"""
        raise NotImplementedError

    def create_query(self) -> orm.Query:
        """Should return an instance of a base query.

        This base query should yield all medical record numbers in the
        base table. Conditions can be added through the clause attribute.
        """
        raise NotImplementedError

    def _fetch_mrns(self, limit=None):
        q = self.create_query()
        if limit:
            q = q.limit(limit)

        mrn_df = read_with_progress(q.statement, self.engine)
        assert len(mrn_df.columns) == 1, "create_query should return only MRNs"
        result = set(
            mrn for mrn in
            mrn_df.iloc[:, 0]
        )
        return result

    def _fetch_data(self, included_mrns=None, limit=None):
        q = self.create_query()
        if included_mrns:
            q = q.filter(self.mrn_column.in_(included_mrns))
        if limit:
            q = q.limit(limit)
        q = q.with_entities(*self.data_columns).distinct()

        result = read_with_progress(
            q.statement, self.engine, silent=True)
        return result

    def example_values(self):
        return self.get_data(limit=10)

    def values_per(self, *columns):
        return self._grouped_count('*', *columns, label='values')

    def patients_per(self, *columns):
        return self._grouped_count(
            self.mrn_column.distinct(),
            *columns,
            label='patients'
        )

    def _grouped_count(self, count_column, *columns, label=None):
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self.create_query()
        q = q.group_by(
            *columns
        ).with_entities(
            *columns
        ).order_by(
            func.count(count_column).label((label or 'count')).desc()
        )

        return read_with_progress(q.statement, self.engine)

    def distinct(self, *columns):
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self.create_query()
        q = q.with_entities(*columns).distinct()

        return read_with_progress(q.statement, self.engine)

    def __or__(self, other):
        if (
            self.base_table == other.base_table
            and not (self._mrns or other._mrns)
        ):
            unique_columns = []
            for col in (self.data_columns+other.data_columns):
                if col not in unique_columns:
                    unique_columns.append(col)

            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause | other.clause,
                data_columns=unique_columns,
                children=[self, other],
                operator=BaseCondition.OR,
            )
        else:
            return BaseCondition(
                mrns=self.get_mrns() | other.get_mrns(),
                children=[self, other],
                operator=BaseCondition.OR,
            )

    def __and__(self, other):
        return self.__class__(
            mrns=self.get_mrns() & other.get_mrns(),
            dimensions=self.dimensions | other.dimensions,
            children=[self, other],
            operator=BaseCondition.AND,
        )

    def __repr__(self):
        if self._mrns:
            return f'{self.__class__.__name__}: {len(self.get_mrns())} mrns'
        else:
            clause = (
                compile_sqla(self.clause, engine) if fiber.VERBOSE
                else '...'
            )
            return (
                f'{self.__class__.__name__} '
                f'({clause})'
            )
