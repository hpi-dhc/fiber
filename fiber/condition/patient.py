from sqlalchemy import orm

from fiber.database.table import d_pers
from fiber.condition import DatabaseCondition


class Patient(DatabaseCondition):

    base_table = d_pers

    def base_query(self):
        return orm.Query(self.base_table).with_entities(
                d_pers.MEDICAL_RECORD_NUMBER
        ).distinct()

    def __init__(
        self, gender=None, religion=None,
        **kwargs
    ):
        super().__init__(**kwargs)

        if gender:
            self.clause &= d_pers.GENDER == gender
        if religion:
            self.clause &= d_pers.RELIGION == religion

    @property
    def _default_columns(self):
        return {
            d_pers.MEDICAL_RECORD_NUMBER,
            d_pers.GENDER,
            d_pers.RELIGION,
        }
