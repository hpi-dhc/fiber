from fiber.condition.database import _case_insensitive_like
from fiber.condition.fact.fact import FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    d_meta,
)


class MetaData(FactCondition):

    dimensions = {'METADATA'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_meta.LEVEL3,
        d_meta.LEVEL4,
        fact.VALUE,
    ]

    def __init__(self, description: str = '', **kwargs):
        if description:
            kwargs['description'] = description
        super().__init__(**kwargs)

    def create_clause(self):
        clause = super().create_clause()
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

    def __init__(self, **kwargs):
        kwargs['description'] = 'Alcohol Use'
        super().__init__(**kwargs)


class TobaccoUse(MetaData):

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
        kwargs['description'] = 'Tobacco Use'
        self._attrs['map_values'] = map_values
        super().__init__(**kwargs)

    def _fetch_data(self, included_mrns=None, limit=None):
        df = super()._fetch_data(included_mrns, limit=limit)
        if self._attrs['map_values']:
            df['value'] = df.value.map(self.MAPPING)
        return df


class DrugUse(MetaData):

    def __init__(self, **kwargs):
        kwargs['description'] = 'Drug Use'
        super().__init__(**kwargs)
