from sqlalchemy import sql

from fiber.condition.database import _case_insensitive_like
from fiber.condition.fact.fact import FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    d_enc,
)


class Encounter(FactCondition):

    dimensions = {'ENCOUNTER'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_enc.ENCOUNTER_TYPE,
        d_enc.ENCOUNTER_CLASS,
        d_enc.BEGIN_DATE_AGE_IN_DAYS,
        d_enc.END_DATE_AGE_IN_DAYS,
    ]

    def __init__(self, category: str = '', **kwargs):
        if category:
            kwargs['category'] = category
        super().__init__(**kwargs)

    def create_clause(self):
        clause = sql.true()
        if self.category:
            clause &= _case_insensitive_like(
                d_enc.ENCOUNTER_TYPE, self.category)
        return clause
