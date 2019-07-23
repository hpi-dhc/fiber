from typing import Set

from sqlalchemy import (
    func,
    orm,
    sql,
)

from fiber import VERBOSE
from fiber.condition.base import BaseCondition
from fiber.database import read_with_progress
from fiber.database.hana import (
    engine,
    compile_sqla,
    session_scope,
)
from fiber.database.table import Table
from fiber.utils import Timer


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
    ):
        self._cached_mrns = mrns or set()
        self.dimensions = dimensions or set()
        # sql.true() acts as an 'empty' initializer for the clause
        self.clause = sql.true() if clause is None else clause
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

        print(f'Executing: {compile_sqla(q)}')
        mrn_df = read_with_progress(q.statement, self.engine)
        assert len(mrn_df.columns) == 1, "create_query should return only MRNs"
        result = set(
            mrn for mrn in
            mrn_df.iloc[:, 0]
        )
        return result

    def get_data(self, inclusion_mrns=None, limit=None):
        request_hash = (
            hash(frozenset(inclusion_mrns or {'All'})),
            hash(frozenset(set(self.data_columns))),
        )

        if request_hash not in self._data_cache:
            q = self.create_query()
            if inclusion_mrns:
                q = q.filter(self.mrn_column.in_(inclusion_mrns))
            if limit:
                q = q.limit(limit)
            q = q.with_entities(*self.data_columns).distinct()
            # print(f'Executing: {compile_sqla(q)}')
            result = read_with_progress(
                q.statement, self.engine)
            self._data_cache[request_hash] = result

        return self._data_cache[request_hash]

    def example_values(self):
        return self.get_data(limit=10)

    def value_counts(self, *columns):
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self.create_query()
        q = q.group_by(
            *columns
        ).with_entities(
            *columns
        ).order_by(
            func.count(self.mrn_column).label('count').desc()
        )

        print(f'Executing: {compile_sqla(q)}')
        return read_with_progress(q.statement, self.engine)

    def distinct(self, *columns):
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self.create_query()
        q = q.with_entities(*columns).distinct()

        print(f'Executing: {compile_sqla(q)}')
        return read_with_progress(q.statement, self.engine)

    def __or__(self, other):
        if (
            self.base_table == other.base_table
            and not (self._cached_mrns or other._cached_mrns)
        ):
            unique_columns = []
            for col in (self.data_columns+other.data_columns):
                if col not in unique_columns:
                    unique_columns.append(col)

            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause | other.clause,
                data_columns=unique_columns,
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
            clause = compile_sqla(self.clause) if VERBOSE else '...'
            return (
                f'{self.__class__.__name__}( '
                f'Not yet executed: {clause})'
            )

    def __len__(self):
        if self._cached_mrns:
            return len(self.get_mrns())
        else:
            with session_scope() as session, Timer() as t:
                q = self.create_query().with_session(session)
                print(f'Executing: {compile_sqla(q)}')
                count = q.count()
            print(f'Execution time: {t.elapsed:.2f}s')
            return count
