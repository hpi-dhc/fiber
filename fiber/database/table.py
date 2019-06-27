from sqlalchemy.sql.schema import Table as SQLATable

from fiber.database.hana import get_meta

META = get_meta()


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
d_enc = Table('D_ENCOUNTER')
