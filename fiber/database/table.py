from sqlalchemy.sql.schema import Table as SQLATable

from fiber.database import get_meta


class Table(SQLATable):

    def __getattr__(self, attr):
        attr = attr.upper()

        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError(f"Table {self.name} has no column {attr}")

    def __new__(cls, name):
        meta = get_meta()
        cls.META = meta
        name = f'{meta.schema}.{name}' if meta.schema else name
        table = cls.META.tables[f'{name}']
        table.__class__ = cls

        return table


fact = Table('FACT')

b_proc = Table('B_PROCEDURE')
fd_proc = Table('FD_PROCEDURE')

b_diag = Table('B_DIAGNOSIS')
fd_diag = Table('FD_DIAGNOSIS')

b_mat = Table('B_MATERIAL')
fd_mat = Table('FD_MATERIAL')

d_pers = Table('D_PERSON')
d_enc = Table('D_ENCOUNTER')
d_uom = Table('D_UNIT_OF_MEASURE')

d_meta = Table('D_METADATA')
d_meta.LEVEL1 = d_meta.LEVEL1_CONTEXT_NAME
d_meta.LEVEL2 = d_meta.LEVEL2_EVENT_NAME
d_meta.LEVEL3 = d_meta.LEVEL3_ACTION_NAME
d_meta.LEVEL4 = d_meta.LEVEL4_FIELD_NAME

epic_lab = Table('EPIC_LAB')
