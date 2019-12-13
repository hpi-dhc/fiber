from enum import Enum
from functools import reduce
from typing import Optional, Set, Union

import pandas as pd
from sqlalchemy import or_

from fiber.condition.database import _case_insensitive_like
from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_meta,
    d_pers,
    fact,
)


class MetaData(_FactCondition):
    """
    MetaData are parts of the building-blocks of FIBER. In order to define
    Cohorts, MetaData identifies 'conditions' that are not easily or commonly
    stored in EHR-DB's the same way.

    It is used to query for conditions that are not purely stored as diagnoses,
    like alcohol- or tobacco-use in the respective subclasses.

    The MetaData adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'METADATA' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, LEVEL3, LEVEL4 and the VALUE in this case respectively.
    """
    dimensions = {'METADATA'}

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_meta.LEVEL3,
        d_meta.LEVEL4,
        fact.VALUE,
    ]

    def __init__(self, name: Optional[str] = '', **kwargs):
        """
        Args:
            name: the name to query for, where either LEVEL1,
                LEVEL2, LEVEL3 or LEVEL4 of the metadata are case-insensitive
                like the parameter provided
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        super().__init__(**kwargs)
        if name:
            self._attrs['name'] = name

    def _create_clause(self):
        clause = super()._create_clause()
        """
        Used to create a SQLAlchemy clause based on the Metadata-condition.
        It is used to select the correct metadata-entries based on the
        name provided at initialization-time.
        """
        if self._attrs['name']:
            clause &= (
                _case_insensitive_like(
                    d_meta.LEVEL1, self._attrs['name']) |
                _case_insensitive_like(
                    d_meta.LEVEL2, self._attrs['name']) |
                _case_insensitive_like(
                    d_meta.LEVEL3, self._attrs['name']) |
                _case_insensitive_like(
                    d_meta.LEVEL4, self._attrs['name'])
            )
        return clause


class AlcoholUse(MetaData):
    """
    The AlcoholUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and name 'Alcohol use'.
    """
    def __init__(self, **kwargs):
        """
        Args:
             kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['name'] = 'Alcohol Use'
        super().__init__(**kwargs)


class TobaccoUse(MetaData):
    """
    The TobaccoUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and names mapping to tobacco use, now or
    sometimes in the past, with name 'Tobacco Use'.
    """

    # Inheriting from str and Enum enables json serialization
    class Type(str, Enum):
        YES = 'Yes'
        NO = 'No'
        FORMER = 'Former'
        UNKNOWN = 'Unknown'
        PASSIVE = 'Passive'

    MAPPING = {
        Type.YES: [
            'Yes',
            'Current Every Day Smoker',
            'Current Some Day Smoker',
            'Current Everyday Smoker',
            'Light Tobacco Smoker',
            'Heavy Tobacco Smoker',
            'Smoker, Current Status Unknown',
        ],
        Type.NO: [
            'Never Smoker',
            'Never',
        ],
        Type.FORMER: [
            'Former Smoker',
            'Quit',
        ],
        Type.UNKNOWN: [
            'Never Assessed',
            'Not Asked',
            'Unknown If Ever Smoked',
        ],
        Type.PASSIVE: [
            'Passive Smoke Exposure - Never Smoker',
            'Passive',
            'Passive Smoker',
        ],
    }

    def __init__(
        self,
        use: Optional[Union[str, Type]] = None,
        map_values: Optional[bool] = True,
        **kwargs,
    ):
        """
        Args:
            use: filter criteria for specific types of use
            map_values: whether to map the values found to the built-in map
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['name'] = 'Tobacco Use'
        super().__init__(**kwargs)
        self._attrs['use'] = TobaccoUse.Type(use) if use else use
        self._attrs['map_values'] = map_values

    def _create_clause(self):
        clause = super()._create_clause()

        if self._attrs['use']:
            value_clauses = [
                fact.VALUE == label
                for label in self.MAPPING[self._attrs['use']]
            ]
            clause &= reduce(or_, value_clauses)

        return clause

    def _fetch_data(self,
                    included_mrns: Optional[Set] = None,
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
            df['value'] = (
                df.value
                .map({
                    label: cat for cat, labels in self.MAPPING.items()
                    for label in labels
                })
                .astype(pd.api.types.CategoricalDtype(self.Type))
            )
        return df


class DrugUse(MetaData):
    """
    The DrugUse adds functionality to the Metadata-Condition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table with
    dimension 'METADATA' and name 'Drug Use'.
    """
    def __init__(self, **kwargs):
        """
        Args:
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        kwargs['name'] = 'Drug Use'
        super().__init__(**kwargs)
