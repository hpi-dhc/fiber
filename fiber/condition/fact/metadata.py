from fiber.condition.database import _case_insensitive_like
from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    d_meta,
)

from typing import Set, Optional


class MetaData(_FactCondition):
    """
    The MetaData adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'METADATA' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, LEVEL3, LEVEL4 and the VALUE in this case respectively.

    It is used to query for conditions that are not purely stored as diagnoses,
    like alcohol- or tobacco-use in the respective subclasses.
    """
    dimensions = {'METADATA'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_meta.LEVEL3,
        d_meta.LEVEL4,
        fact.VALUE,
    ]

    def __init__(self, description: str = '', **kwargs):
        """
        Args:
            description: the description to query for, where either LEVEL1,
                LEVEL2, LEVEL3 or LEVEL4 of the metadata are case-insensitive
                like the parameter provided
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)

    def _create_clause(self):
        clause = super()._create_clause()
        """
        Used to create a SQLAlchemy clause based on the Metadata-condition.
        It is used to select the correct metadata-entries based on the
        description provided at initialization-time.
        :return:
        """
        if self._attrs['description']:
            clause &= (
                _case_insensitive_like(
                    d_meta.LEVEL1, self._attrs['description']) |
                _case_insensitive_like(
                    d_meta.LEVEL2, self._attrs['description']) |
                _case_insensitive_like(
                    d_meta.LEVEL3, self._attrs['description']) |
                _case_insensitive_like(
                    d_meta.LEVEL4, self._attrs['description'])
            )
        return clause


class AlcoholUse(MetaData):
    """
    The AlcoholUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and description 'Alcohol use'.
    """
    def __init__(self, **kwargs):
        """
        Args:
             kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['description'] = 'Alcohol Use'
        super().__init__(**kwargs)


class TobaccoUse(MetaData):
    """
    The TobaccoUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and descriptions mapping to tobacco use, now or
    sometimes in the past, with description 'Tobacco Use'.
    """
    MAPPING = {
        'Yes': 'Yes',
        'Current Every Day Smoker': 'Yes',
        'Current Some Day Smoker': 'Yes',
        'Current Everyday Smoker': 'Yes',
        'Light Tobacco Smoker': 'Yes',
        'Heavy Tobacco Smoker': 'Yes',
        'Smoker, Current Status Unknown': 'Yes',
        'Never Smoker': 'No',
        'Never': 'No',
        'Former Smoker': 'Former',
        'Quit': 'Former',
        'Never Assessed':  'Unknown',
        'Not Asked': 'Unknown',
        'Unknown If Ever Smoked': 'Unknown',
        'Passive Smoke Exposure - Never Smoker': 'Passive',
        'Passive': 'Passive',
        'Passive Smoker': 'Passive',
    }

    def __init__(self, map_values: bool = True, **kwargs):
        """
        :param map_values: whether to map the values found to the built-in map
        :param kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['description'] = 'Tobacco Use'
        self._attrs['map_values'] = map_values
        super().__init__(**kwargs)

    def _fetch_data(self,
                    included_mrns: Set = None,
                    limit: Optional[int] = None):
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
            df['value'] = df.value.map(self.MAPPING)
        return df


class DrugUse(MetaData):
    """
    The DrugUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and description 'Drug Use'.
    """
    def __init__(self, **kwargs):
        """
        Args:
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['description'] = 'Drug Use'
        super().__init__(**kwargs)
