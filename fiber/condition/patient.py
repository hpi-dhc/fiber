from sqlalchemy import orm, extract

from fiber.database.table import d_pers, fact
from fiber.condition import DatabaseCondition, BaseCondition
from fiber.condition.database import _case_insensitive_like


class Patient(DatabaseCondition):
    """
    The patient is based of the DatabaseCondition and accesses the D_Person
    table of MSDW. It contains general information about the patients.
    (e.g. YEAR_OF_BIRTH, MONTH_OF_BIRTH, GENDER, ADDRESS_ZIP, ...)
    """
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
    ]
    mrn_column = d_pers.MEDICAL_RECORD_NUMBER
    age_in_days = fact.AGE_IN_DAYS

    def __init__(
        self, gender=None, religion=None, race=None,
        **kwargs
    ):
        """
        Args:
            String gender: The gender of the Patient (e.g. 'Male','Female')
            String religion:
                The religion of the Patient (e.g. 'Hindu','Catholic')
            String race: The race of the Patient (e.g. 'Hispanic/Latino')

        The string parameters are used in the SQL-LIKE statement after being
        converted to uppercase. This means that  ``%``, ``_`` and  ``[]`` can
        be used to more precisly select patients.
        """
        super().__init__(**kwargs)
        self._attrs['gender'] = gender
        self._attrs['religion'] = religion
        self._attrs['race'] = race

    def _create_clause(self):
        clause = super()._create_clause()
        """
        Used to create a SQLAlchemy clause based on the Patient-condition.
        It is used to select the correct patients based on the category
        provided at initialization-time.
        :return:
        """

        if self._attrs['gender']:
            clause &= _case_insensitive_like(
                d_pers.GENDER, self._attrs['gender'])
        if self._attrs['religion']:
            clause &= _case_insensitive_like(
                d_pers.RELIGION, self._attrs['religion'])
        if self._attrs['race']:
            clause &= _case_insensitive_like(
                d_pers.RACE, self._attrs['race'])

        # cast(extract('year', dp.DATE_OF_BIRTH),sqlalchemy.Integer) != 1066
        # currentYear = datetime.datetime.now().year
        return clause

    def _create_query(self):
        """
        Creates an instance of a SQLAlchemy query which only returns MRNs.

        This query should yield all medical record numbers in the
        ``base_table`` of the condition. It uses the ``.clause`` to select
        the relevant patients.

        This query is also used by other functions which change the selected
        columns to get data about the patients.
        """

        return orm.Query(self.base_table).filter(
            self.clause
        ).filter(
            d_pers.ACTIVE_FLAG == 'Y'
        ).with_entities(
            self.mrn_column
        ).distinct()

    def __and__(self, other):
        """
        The Patient has its own __and__ function, because the SQL can be easily
        combined to optimize performance.
        """
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
