from sqlalchemy import orm, extract, sql

from fiber.database.table import d_pers, fact
from fiber.condition import DatabaseCondition, BaseCondition
from fiber.condition.database import _case_insensitive_like


class Patient(DatabaseCondition):

    base_table = d_pers
    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        extract('year', d_pers.DATE_OF_BIRTH).label('YEAR_OF_BIRTH'),
        d_pers.MONTH_OF_BIRTH,
        d_pers.GENDER,
        d_pers.RELIGION,
        d_pers.RACE,
        d_pers.PATIENT_ETHNIC_GROUP,
        d_pers.DECEASED_INDICATOR,
        d_pers.MOTHER_ACCOUNT_NUMBER,
        d_pers.ADDRESS_ZIP,
        d_pers.MARITAL_STATUS_CODE,
        d_pers.ADDRESS_ZIP,
    ]
    mrn_column = d_pers.MEDICAL_RECORD_NUMBER
    age_in_days = fact.AGE_IN_DAYS

    def __init__(
        self, gender=None, religion=None, race=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.gender = gender
        self.religion = religion
        self.race = race

    def create_clause(self):
        clause = sql.true()
        if self.gender:
            clause &= _case_insensitive_like(d_pers.GENDER, self.gender)
        if self.religion:
            clause &= _case_insensitive_like(d_pers.RELIGION, self.religion)
        if self.race:
            clause &= _case_insensitive_like(d_pers.RACE, self.race)

        # cast(extract('year', dp.DATE_OF_BIRTH),sqlalchemy.Integer) != 1066
        # currentYear = datetime.datetime.now().year
        return clause

    def __getstate__(self):
        if self.children:
            return BaseCondition.__getstate__(self)
        else:
            return {
                'class': self.__class__.__name__,
                'attributes': {
                    'gender': self.gender,
                    'religion': self.religion,
                    'race': self.race,
                },
            }

    def create_query(self):
        return orm.Query(self.base_table).filter(
            self.clause
        ).filter(
            d_pers.ACTIVE_FLAG == 'Y'
        ).with_entities(
            self.mrn_column
        ).distinct()

    def __and__(self, other):
        '''
        The Patient has its own __and__ function, because they can be easily
        combined. This optimizes performance.
        '''
        if (
            isinstance(other, Patient)
            and not (self._mrns or other._mrns)
        ):
            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause & other.clause,
                children=[self, other],
                operator=BaseCondition.AND,
            )
        else:
            return super().__and__(self, other)
