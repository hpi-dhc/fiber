from abc import ABC
from typing import Optional

import fiber
from fiber.condition.base import _BaseCondition


class AgeMixin(ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._attrs['age_conditions'] = []

    def _create_clause(self):
        clause = super()._create_clause()

        if fiber.config.MIN_DAYS_FILTER_ACTIVE:
            clause &= self.base_table.AGE_IN_DAYS > 0

        for a in self._attrs['age_conditions']:
            if a['min_days']:
                clause &= self.base_table.AGE_IN_DAYS >= a['min_days']
            if a['max_days']:
                clause &= self.base_table.AGE_IN_DAYS < a['max_days']

        return clause

    def age(self,
            min_age: Optional[int] = None,
            max_age: Optional[int] = None):
        self._attrs['age_conditions'].append({
            'min_days': 365 * min_age if min_age else None,
            'max_days': 365 * max_age if max_age else None
        })
        return self

    def age_in_days(self,
                    min_days: Optional[int] = None,
                    max_days: Optional[int] = None):
        self._attrs['age_conditions'].append({
            'min_days': min_days,
            'max_days': max_days
        })
        return self

    @classmethod
    def from_dict(cls: _BaseCondition, json: dict):
        c = super().from_dict(json)
        for a in json['attributes'].get('age_conditions', []):
            c.age_in_days(a['min_days'], a['max_days'])
        return c
