from sqlalchemy import orm

from fiber.database.table import d_pers, fact
from fiber.condition import DatabaseCondition


class Patient(DatabaseCondition):

    base_table = d_pers
    _default_columns = {
        d_pers.MEDICAL_RECORD_NUMBER,
        d_pers.GENDER,
        d_pers.RACE,
        d_pers.PATIENT_ETHNIC_GROUP,
    }
    mrn_column = d_pers.MEDICAL_RECORD_NUMBER
    age_in_days = fact.AGE_IN_DAYS

    def __init__(
        self, gender=None, religion=None, race=None,
        **kwargs
    ):
        super().__init__(**kwargs)

        if gender:
            self.clause &= d_pers.GENDER == gender
        if religion:
            self.clause &= d_pers.RELIGION == religion
        if race:
            self.clause &= d_pers.RACE == race

    def create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).filter(
            d_pers.ACTIVE_FLAG == 'Y'
        ).with_entities(
                self.mrn_column
        ).distinct()

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
