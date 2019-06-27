from typing import Set

import pandas as pd
from sqlalchemy import orm, sql
from contexttimer import Timer

from fiber.condition.base import BaseCondition

from fiber.database.table import Table
from fiber.database.hana import session_scope, compile_sqla


class DatabaseCondition(BaseCondition):

    def __init__(
        self,
        mrns: Set[str] = None,
        dimensions: Set[str] = None,
        clause=None,
        data_columns=None,
    ):
        self._cached_mrns = mrns or set()
        self.dimensions = dimensions or set()
        # sql.true() acts as an 'empty' initializer for the clause
        self.clause = sql.true() if clause is None else clause
        self._specified_columns = data_columns or set()

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
        """ Set of default columns of the database data query"""
        raise NotImplementedError

    @property
    def data_columns(self):
        """Returns the column selection for fetching data points."""
        return self._specified_columns or self._default_columns

    def create_query(self) -> orm.Query:
        """Should return an instance of a base query.

        This base query should yield all medical record numbers in the
        base table. Conditions can be added through the clause attribute.
        """
        raise NotImplementedError

    def mrn_filter(self, mrns):
        """Returns a clause that restricts the query to a set of mrns."""
        raise NotImplementedError

    def _fetch_mrns(self):
        with session_scope() as session:
            q = self.create_query()
            q = q.with_session(session)

            print(f'Executing: {compile_sqla(q)}')
            with Timer() as t:
                result = set(mrn[0] for mrn in q.all())
            print('Execution time:', t.elapsed)
            return result

    def get_data(self, inclusion_mrns):
        with session_scope() as session:
            q = self.create_query()
            q = q.filter(self.mrn_filter(inclusion_mrns))
            q = q.with_entities(*self.data_columns).distinct()

            print(f'Executing: {compile_sqla(q)}')
            with Timer() as t:
                result = pd.read_sql(q.statement, session.connection())
            print('Execution time:', t.elapsed)
            return result

    def __or__(self, other):
        if (
            self.base_table == other.base_table
            and not (self._cached_mrns or other._cached_mrns)
        ):
            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause | other.clause,
                data_columns=self.data_columns | other.data_columns,
            )
        else:
            return BaseCondition(mrns=self.get_mrns() | other.get_mrns())

    def __and__(self, other):
        return self.__class__(
            mrns=self.get_mrns() & other.get_mrns(),
            dimensions=self.dimensions | other.dimensions,
        )

    def __repr__(self):
        if self._cached_mrns:
            return f'{self.__class__.__name__}: {len(self.get_mrns())} mrns'
        else:
            return (
                f'{self.__class__.__name__}: '
                f'Not executed: \n\t{compile_sqla(self.clause)}'
            )
