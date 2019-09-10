from functools import reduce
from itertools import chain
from typing import Optional, Set

import pandas as pd
from sqlalchemy import (
    func,
    or_,
    orm,
    sql,
)

import fiber
from fiber.condition.base import _BaseCondition
from fiber.database import (
    compile_sqla,
    read_with_progress,
)
from fiber.database import get_engine
from fiber.database.table import Table


def _case_insensitive_like(column: str, value):
    return func.upper(column).like(value.upper())


def _multi_like_clause(column: str, value_or_values):
    values = (
        [value_or_values]
        if isinstance(value_or_values, str)
        else value_or_values
    )

    return reduce(
        or_,
        [_case_insensitive_like(column, value) for value in values]
    )


class _DatabaseCondition(_BaseCondition):
    """
    The DatabaseCondition adds functionality to the BaseCondition which
    is needed to run queries against a database. It also allows to combine
    SQL Statements into one to optimize performance. It should only be used by
    developers and not by end-users. It builds the basis for specific
    conditions like Diagnosis, VitalSign, ...

    It should be possible to use this for other databases that use MRNs by
    adjusting the engine. Problems one would need to look into is that database
    conditions of different DBs are only combined as BaseConditions in
    ``__and__``, ``__or__``.
    """

    engine = get_engine()

    def __init__(
        self,
        mrns: Optional[Set[str]] = None,
        dimensions: Optional[Set[str]] = None,
        clause=None,
        data_columns=None,
        **kwargs
    ):
        """
        Args:
            Set mrns: Set of MRN-Strings for which the condition is true.
            List children: List of child conditions which were combined with an
                operator.
            Str operator: String representing the combination of the child
                condition (e.g. ``_BaseCondition.AND``)
            Set dimensions: A set of tables that need to be joined on the
                ``base_table``
            ClauseElement clause: The SQLAlchemy clause of the current
                condition to select patients.
            ColumnElement data_columns: The SQLAlchemy data_columns that should
                be returned when ``.get_data()`` is called.

        In our case ``dimension`` and ``clause`` are only used on the fact
        table. To minimize the executed joins and queries. This might be
        extendable for other star schemas.
        """
        super().__init__(**kwargs)
        self.dimensions = dimensions or set()
        # sql.true() acts as an 'empty' initializer for the clause
        self._clause = sql.true() if clause is None else clause
        self.data_columns = data_columns or []
        self._data_cache = {}

    @property
    def base_table(self) -> Table:
        """
        Must be set by subclasses to the database table which the class uses to
        select patients and data. This is also used to optimize queries on the
        same table.
        """
        raise NotImplementedError

    @property
    def _default_columns(self):
        """
        Must be set by subclasses.

        This should return an array of columns which are in the result table of
        ``._create_query()``. These columns will be returned by default when
        ``.get_data()`` is called.
        """
        raise NotImplementedError

    @property
    def mrn_column(self):
        """
        Must be set by subclasses.

        This is used to specify the column in the result table of
        ``._create_query()`` which is holding the MRNs.
        """
        raise NotImplementedError

    @property
    def data_columns(self):
        """
        Returns columns which are in the result table of
        ``._create_query()``. These columns will be returned when
        ``.get_data()`` is called.
        """
        return [
            str(col) for col
            in (self._specified_columns or self._default_columns)
        ]

    @data_columns.setter
    def data_columns(self, value):
        self._specified_columns = value

    @property
    def clause(self):
        """
        Returns the clause of the current condition or runs
        ``._create_clause()`` to create it.
        """
        # TODO recursively create clause of combinable conditions
        if not isinstance(self._clause, sql.elements.True_):
            return self._clause
        else:
            return self._create_clause()

    def _create_clause(self):
        """
        Should be overwritten by subclasses to create a SQLAlchemy clause based
        on the defined condition. It is used to select the correct patients.
        """
        return sql.true()

    def _create_query(self) -> orm.Query:
        """
        Must be implemented by subclasses to return an instance of a SQLAlchemy
        query which only returns MRNs.

        This query should yield all medical record numbers in the
        ``base_table`` of the condition. It uses the ``.clause`` to select
        the relevant patients.

        This query is also used by other function which change the selected
        columns to get data about the patients.
        """
        raise NotImplementedError

    def _fetch_mrns(self,
                    limit: Optional[int] = None):
        """Fetches MRNs from the results of ``._create_query()``."""
        q = self._create_query()
        if limit:
            q = q.limit(limit)

        mrn_df = read_with_progress(q.statement, self.engine)
        if mrn_df.empty:
            mrn_df = pd.DataFrame(columns=['medical_record_number'])
        assert len(mrn_df.columns) == 1, '_create_query must return only MRNs'
        result = set(
            mrn for mrn in
            mrn_df.iloc[:, 0]
        )
        return result

    def _fetch_data(self,
                    included_mrns: Optional[Set] = None,
                    limit: Optional[int] = None):
        """
        Fetches the data defined with ``.data_columns`` for each patient
        defined by this condition and via ``included_mrns`` from the results of
        ``._create_query()``.
        """
        q = self._create_query()
        if included_mrns:
            q = q.filter(self.mrn_column.in_(included_mrns))
        if limit:
            q = q.limit(limit)
        q = q.with_entities(*self.data_columns).distinct()

        result = read_with_progress(
            q.statement, self.engine, silent=bool(included_mrns))
        return result

    def example_values(self):
        """
        Returns ten values of the current condition.

        Example:
            >>> Patient(gender='Female', religion='Hindu').example_values()
        """
        return self.get_data(limit=10)

    def values_per(self, *columns: Set[str]):
        """
        Counts occurence of unique values in the specified columns.
        """
        return self._grouped_count('*', *columns, label='values')

    def patients_per(self, *columns: Set[str]):
        """
        Counts distinct patients for unique values in the specified columns.
        """
        return self._grouped_count(
            self.mrn_column.distinct(),
            *columns,
            label='patients'
        )

    def _grouped_count(self,
                       count_column: str,
                       *columns: Set[str],
                       label: Optional[str] = None):
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self._create_query()
        q = q.group_by(
            *columns
        ).with_entities(
            *columns
        ).order_by(
            func.count(count_column).label((label or 'count')).desc()
        )

        return read_with_progress(q.statement, self.engine)

    def distinct(self, *columns: Set[str]):
        """Returns distinct values based on the specified ``columns``"""
        if not columns:
            raise ValueError('Supply one or multiple columns as arguments.')

        q = self._create_query()
        q = q.with_entities(*columns).distinct()

        return read_with_progress(q.statement, self.engine)

    def to_dict(self):
        obj_dict = super().to_dict()
        if self._specified_columns:
            obj_dict['data_columns'] = self.data_columns

        return obj_dict

    @classmethod
    def from_dict(cls: _BaseCondition, obj_dict: dict):
        obj = super().from_dict(obj_dict)
        if 'data_columns' in obj_dict:
            obj.data_columns = obj_dict['data_columns']
        return obj

    def __or__(self, other: _BaseCondition):
        """
        The _DatabaseCondition optimizes the SQL statements for ``|`` by
        combining the clauses of condition which run on the same database
        table. This is done via the ``.base_table`` attribute.
        """
        if (
            self.base_table == other.base_table
            and not (self._mrns or other._mrns)
        ):
            unique_columns = list(dict.fromkeys(
                chain(self.data_columns, other.data_columns)
            ))

            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause | other.clause,
                data_columns=unique_columns,
                children=[self, other],
                operator=_BaseCondition.OR,
            )
        else:
            return _BaseCondition(
                mrns=self.get_mrns() | other.get_mrns(),
                children=[self, other],
                operator=_BaseCondition.OR,
            )

    def __and__(self, other: _BaseCondition):
        # The SQL queries could theoretically be combined for AND as well, by
        # running them as subqueries and joining on the MRNs
        return self.__class__(
            mrns=self.get_mrns() & other.get_mrns(),
            dimensions=self.dimensions | other.dimensions,
            children=[self, other],
            operator=_BaseCondition.AND,
        )

    def __repr__(self):
        """Shows the running query or the resulting MRNs"""
        if self._mrns:
            return f'{self.__class__.__name__}: {len(self.get_mrns())} mrns'
        else:
            clause = (
                compile_sqla(self.clause, self.engine) if fiber.VERBOSE
                else '...'
            )
            return (
                f'{self.__class__.__name__} '
                f'({clause})'
            )
