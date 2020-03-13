from enum import Enum
from functools import reduce
from typing import Optional, Set

import pandas as pd
from sqlalchemy import or_, orm

from fiber.condition import _BaseCondition, _DatabaseCondition
from fiber.condition.database import _case_insensitive_like
from fiber.database.table import d_pers, fact


class Patient(_DatabaseCondition):
    """
    Patients are one basic building-block of FIBER. When querying EHR-DB's
    one wants to define Cohorts, which are basically the groups of patients,
    identified via MRN, in order to run analysis on or to use these for machine
    learning.

    The patient is based of the _DatabaseCondition and accesses the D_Person
    table of MSDW. It contains general information about the patients.
    (e.g. YEAR_OF_BIRTH, MONTH_OF_BIRTH, GENDER, ADDRESS_ZIP, ...)
    """
    base_table = d_pers
    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        d_pers.DATE_OF_BIRTH,
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
    age_column = fact.AGE_IN_DAYS

    def __init__(
        self,
        gender: Optional[str] = None,
        religion: Optional[str] = None,
        race: Optional[str] = None,
        map_values: Optional[bool] = None,
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
        self._attrs['map_values'] = map_values

    def _create_clause(self):
        clause = super()._create_clause()
        """
        Used to create a SQLAlchemy clause based on the Patient-condition.
        It is used to select the correct patients based on the category
        provided at initialization-time.
        """

        if self._attrs['gender']:
            clause &= _case_insensitive_like(
                d_pers.GENDER, self._attrs['gender'])
        if self._attrs['religion']:
            if self._attrs['map_values']:
                religion_clauses = [
                    _case_insensitive_like(d_pers.RELIGION, label)
                    for label in self.RELIGION_MAPPING[self._attrs['religion']]
                ]
                clause &= reduce(or_, religion_clauses)
            else:
                clause &= _case_insensitive_like(
                    d_pers.RELIGION, self._attrs['religion']
                )
        if self._attrs['race']:
            if self._attrs['map_values']:
                race_clauses = [
                    _case_insensitive_like(d_pers.RACE, label)
                    for label in self.RACE_MAPPING[self._attrs['race']]
                ]
                clause &= reduce(or_, race_clauses)
            else:
                clause &= _case_insensitive_like(
                    d_pers.RACE, self._attrs['race']
                )

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

    def __and__(self, other: _DatabaseCondition):
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
                operator=_BaseCondition.AND,
            )
        else:
            return super().__and__(self, other)

    def _fetch_data(
        self,
        included_mrns: Optional[Set] = None,
        limit: Optional[int] = None
    ):
        """
        Fetches the data defined with ``.data_columns`` for each patient
        defined by this condition and via ``included_mrns`` from the results of
        ``.create_query()``.

        Args:
            included_mrns: the medical record numbers to include
            limit: if the cohort shall be limited in size,
                specify positive integer
            df containing the mapped or unmapped values from the db
        """
        df = super()._fetch_data(included_mrns, limit=limit)
        if self._attrs['map_values']:
            df['race'] = (
                df.race.map({
                    label: cat for cat, labels in self.RACE_MAPPING.items()
                    for label in labels
                }).astype(pd.api.types.CategoricalDtype(self.RaceType))
            )
            df['religion'] = (
                df.religion.map({
                    label: cat for cat, labels in self.RELIGION_MAPPING.items()
                    for label in labels
                }).astype(pd.api.types.CategoricalDtype(self.ReligionType))
            )
        return df

    class RaceType(str, Enum):
        AFRICAN = 'African'
        AMERICAN_BLACK = 'Black or African-American'
        AMERICAN_NATIVE = 'Native American'
        ASIAN = 'Asian'
        ASIAN_PACIFIC = 'Asian Pacific'
        ASIAN_INDIAN = 'Asian Indian'
        ASIAN_CHINESE = 'Asian Chinese'
        HISPANIC = 'Hispanic or Latino'
        OTHER = 'Other'
        WHITE = 'White'

    RACE_MAPPING = {
        RaceType.AFRICAN: [
            'Cape Verdian',
            'Congolese',
            'Eritrean',
            'Ethiopian',
            'Gabonian',
            'Ghanaian',
            'Guinean',
            'Ivory Coastian',
            'Kenyan',
            'Liberian',
            'Madagascar',
            'Malian',
            'Nigerian',
            'Other: East African',
            'Other: North African',
            'Other: South African',
            'Other: West African',
            'Senegalese',
            'Sierra Leonean',
            'Somalian',
            'Sudanese',
            'Tanzanian',
            'Togolese',
            'Ugandan',
            'Zimbabwean'
        ],
        RaceType.AMERICAN_BLACK: [
            'African American (Black)',
            'African-American',
            'Black Or African-American',
            'Black or African - American',
        ],
        RaceType.AMERICAN_NATIVE: [
            'American (Indian/Alaskan)',
            'Native American'
        ],
        RaceType.ASIAN: [
            'Asian',
            'Bangladeshi',
            'Bhutanese',
            'Burmese',
            'Cambodian',
            'Hmong',
            'Indonesian',
            'Japanese',
            'Korean',
            'Laotian',
            'Malaysian',
            'Maldivian',
            'Nepalese',
            'Okinawan',
            'Pakistani',
            'Singaporean',
            'Taiwanese',
            'Thai',
            'Vietnamese',
            'Yapese'
        ],
        RaceType.ASIAN_PACIFIC: [
            'Asian (Pacific Islander)',
            'Carolinian',
            'Chamorro',
            'Chuukese',
            'Fijian',
            'Filipino',
            'Guamanian',
            'Guamanian Or Chamorro',
            'Guamanian or Chamorro',
            'Iwo Jiman',
            'Kiribati',
            'Kosraean',
            'Mariana Islander',
            'Marshallese',
            'Melanesian',
            'Micronesian',
            'Native Hawaiian',
            'New Hebrides',
            'Other Pacific Islander',
            'Pacific Islander',
            'Palauan',
            'Pohnpeian',
            'Polynesian',
            'Saipanese',
            'Samoan',
            'Papua New Guinean',
            'Tahitian',
            'Tokelauan',
            'Tongan'
        ],
        RaceType.ASIAN_INDIAN: [
            'Asian Indian',
            'Sri Lankan',
            'Sri lankan',
            'West Indian'
        ],
        RaceType.ASIAN_CHINESE: [
            'Chinese',
        ],
        RaceType.HISPANIC: [
            'Barbadian',
            'Dominica Islander',
            'Grenadian',
            'Haitian',
            'Hispanic/Latino',
            'Jamaican',
            'St Vincentian',
            'Trinidadian'
        ],
        RaceType.OTHER: [
            '',
            'Aa',
            'Ab',
            'Af',
            'Ag',
            'Ak',
            'Al',
            'Ap',
            'Ar',
            'Av',
            'Ay',
            'B',
            'B1',
            'B2',
            'B3',
            'B4',
            'B5',
            'B6',
            'B7',
            'B8',
            'B9',
            'Ba',
            'Bb',
            'Bc',
            'Bd',
            'Be',
            'Bf',
            'Bg',
            'Bh',
            'Bj',
            'Bk',
            'Bm',
            'Bn',
            'Bo',
            'Bp',
            'Bq',
            'Br',
            'Bs',
            'Bt',
            'Bu',
            'Bv',
            'Bw',
            'Bx',
            'By',
            'Bz',
            'I',
            'MSDW_NOT APPLICABLE',
            'MSDW_OTHER',
            'MSDW_UNKNOWN',
            'NOT AVAILABLE',
            'Non Hispanic',
            'O',
            'Other',
            'Pk',
            'Pl',
            'Pm',
            'Po',
            'Ps',
            'Pv',
            'U',
            'Unk',
            'Unknown',
            'W'
        ],
        RaceType.WHITE: [
            'Caucasian (White)',
            'White'
        ]
    }

    class ReligionType(str, Enum):
        BUDDHISM = 'Buddhism'
        CHRISTIAN = 'Christian'
        HINDU = 'Hindu'
        JEWISH = 'Jewish'
        MUSLIM = 'Muslim'
        NONE = 'None'
        OTHER = 'Other'
        UNKNOWN = 'Unknown'

    RELIGION_MAPPING = {
        ReligionType.BUDDHISM: [
            'Buddhism'
        ],
        ReligionType.CHRISTIAN: [
            'Catholic',
            'Christian',
            'Baptist',
            'Episcopal',
            'Protestant',
            'Pentecostal',
            'Greek Orthodox',
            'Christian Scientist',
            'Methodist',
            'Jehovah\'S Witness',
            'Presbyterian',
            'Jehovah\'s Witness',
            'Lutheran',
            'Seventh Day Adventist',
            'Mormon',
            'Latter Day Saints'
        ],
        ReligionType.HINDU: [
            'Hindu'
        ],
        ReligionType.JEWISH: [
            'Jewish'
        ],
        ReligionType.MUSLIM: [
            'Muslim'
        ],
        ReligionType.NONE: [
            'None',
            'Atheist'
        ],
        ReligionType.OTHER: [
            'Other',
            'Quaker'
        ],
        ReligionType.UNKNOWN: [
            '',
            'Unknown',
            'NOT AVAILABLE',
            'MSDW_UNKNOWN',
            'Pt Declined',
            'PT Declined',
            'MSDW_NOT APPLICABLE',
            'MSDW_OTHER'
        ],
    }
