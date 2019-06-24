from sqlalchemy.orm import Query
from sqlalchemy.sql.schema import Table as SQLATable

from fiber.database.hana import get_meta

META = get_meta()


# class Table:

#     meta = META

#     def __init__(self, name):
#         self._table = self.meta.tables[f'MSDW_2018.{name.lower()}']
#         self._name = name

#     def __getattr__(self, attr):
#         attr = attr.lower()

#         if attr in self._table.columns:
#             return self._table.columns[attr]
#         else:
#             raise AttributeError(f"Table {self._name} has no column {attr}")


class Table(SQLATable):

    def __getattr__(self, attr):
        attr = attr.lower()

        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError(f"Table {self.name} has no column {attr}")

    def __new__(cls, name):
        table = META.tables[f'MSDW_2018.{name.lower()}']
        table.__class__ = cls

        return table


fact = Table('FACT2')

b_proc = Table('B_PROCEDURE')
fd_proc = Table('FD_PROCEDURE')

b_diag = Table('B_DIAGNOSIS')
fd_diag = Table('FD_DIAGNOSIS')

b_mat = Table('B_MATERIAL')
fd_mat = Table('FD_MATERIAL')

d_pers = Table('D_PERSON')

dimensions = {
    'PROCEDURE': (fd_proc, b_proc),
    'MATERIAL': (fd_mat, b_mat),
    'DIAGNOSIS': (fd_diag, b_diag),
}


def fact_query():
    return Query(fact).join(
        d_pers,
        fact.person_key == d_pers.person_key
    ).with_entities(
            d_pers.MEDICAL_RECORD_NUMBER
    ).distinct()


def join_dimension(query, dim):
    assert dim in dimensions
    d_table, b_table = dimensions[dim]

    key = f'{dim}_key'
    group_key = f'{dim}_group_key'

    return query.join(
        b_table,
        getattr(fact, group_key) == getattr(b_table, group_key)
    ).join(
        d_table,
        getattr(d_table, key) == getattr(b_table, key)
    )


def filter_by(query, condition):
    query = query.filter(condition.clause)

    for dimension in condition.dimensions:
        query = join_dimension(query, dimension)
    return query
