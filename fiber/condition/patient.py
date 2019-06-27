from sqlalchemy import orm

from fiber.database.table import d_pers
from fiber.condition import DatabaseCondition


class Patient(DatabaseCondition):

    base_table = d_pers
    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        d_pers.GENDER,
        d_pers.RELIGION,
    }

    def __init__(
        self, gender=None, religion=None,
        **kwargs
    ):
        super().__init__(**kwargs)

        if gender:
            self.clause &= d_pers.GENDER == gender
        if religion:
            self.clause &= d_pers.RELIGION == religion

    def create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).with_entities(
                d_pers.MEDICAL_RECORD_NUMBER
        ).distinct()

    def mrn_filter(self, mrns):
        return d_pers.MEDICAL_RECORD_NUMBER.in_(mrns)

    def __and__(self, other):
        if (
            isinstance(other, Patient)
            and not (self._cached_mrns or other._cached_mrns)
        ):
            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause & other.clause,
                data_columns=self.data_columns | other.data_columns,
            )
        else:
            return super().__and__(self, other)
