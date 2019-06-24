from typing import Set

import pandas as pd
from sqlalchemy import orm, sql

from fiber.condition.base import BaseCondition
from fiber.database.table import (
    filter_by,
    Table,
    d_pers,
)

from fiber.database.hana import session_scope, compile_sqla


class DatabaseCondition(BaseCondition):

    def __init__(
        self,
        mrns: Set[str] = None,
        dimensions: Set[str] = None,
        clause=None,
        columns=None,
    ):
        # sql.true() acts as an 'empty' initializer for the clause
        self._mrns = mrns or set()
        self.dimensions = dimensions or set()
        self.clause = sql.true() if clause is None else clause
        self._specified_columns = columns or set()

    @property
    def base_table(self) -> Table:
        """This property should be set to the base table containing data.

        All queries generated from a `DatabaseCondition` are constraints
        on this base table. If the base table matches, conditions can
        potentially be combined into a single query.
        """
        raise NotImplementedError

    def base_query(self) -> orm.Query:
        """Should return an instance of a base query.

        This base query should yield all medical record numbers in the
        base table. Conditions can be added through the clause attribute.
        """
        raise NotImplementedError

    @property
    def columns(self):
        """Returns the column selection for fetching data points."""
        return self._specified_columns or self._default_columns

    def _fetch_mrns(self):
        with session_scope() as session:
            q = self.base_query()
            q = filter_by(q, self)

            q = q.with_session(session)

            print(f'Executing: {compile_sqla(q)}')
            return set(mrn[0] for mrn in q.all())

    def _fetch_data(self, mrn_constraint_clause):
        with session_scope() as session:
            q = self.base_query()
            q = filter_by(q, self).filter(mrn_constraint_clause)
            q = q.with_entities(*self.columns).distinct()
            print(f'Executing: {compile_sqla(q)}')

            return pd.read_sql(q.statement, session.connection())

    @property
    def mrn_clause(self):
        if self.already_executed:
            return d_pers.MEDICAL_RECORD_NUMBER.in_(self.mrns)
        else:
            q = self.base_query()
            q = filter_by(q, self)

            return d_pers.MEDICAL_RECORD_NUMBER.in_(q)

    def __or__(self, other):
        if (
            self.base_table == other.base_table
            and not (self.already_executed or other.already_executed)
        ):
            return self.__class__(
                mrns=None,
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause | other.clause,
                columns=self.columns | other.columns,
            )
        else:
            return BaseCondition(mrns=self.mrns | other.mrns)

    def __and__(self, other):
        return self.__class__(
            mrns=self.mrns & other.mrns,
            dimensions=self.dimensions | other.dimensions,
        )

    def __repr__(self):
        if self.already_executed:
            return f'{self.__class__.__name__}: {len(self._mrns)} patients'
        else:
            return (
                f'{self.__class__.__name__}: '
                f'Not executed: \n\t{compile_sqla(self.clause)}'
            )
