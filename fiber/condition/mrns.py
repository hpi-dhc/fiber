from typing import Iterable, Union

import pandas as pd

from fiber import OCCURRENCE_INDEX
from fiber.condition.base import BaseCondition


class MRNs(BaseCondition):
    """
    The MRNs-Condition adds functionality to the BaseCondition. It allows to
    combine SQL Statements that shall be performed on the FACT-Table.
    """

    def __init__(
        self,
        mrns: Union[pd.DataFrame, Iterable[str]] = None
    ):
        """
        Args:
            mrns: list of mrns that shall be the elements of selection
        """
        df = pd.DataFrame(columns=OCCURRENCE_INDEX)

        if isinstance(mrns, pd.DataFrame):
            df = df.append(mrns)
        elif isinstance(mrns, Iterable):
            df.medical_record_number = df.medical_record_number.append(
                pd.Series(mrns))

        self._data = df.sort_values(OCCURRENCE_INDEX)
        self._mrns = set(self._data.medical_record_number)

    def _fetch_data(self, included_mrns=None, limit=None):
        """
        Fetches the data defined with ``.data_columns`` for each patient
        defined by this condition and via ``included_mrns`` from the results of
        ``.create_query()``.

        Args:
            included_mrns: the medical record numbers to include
            limit: if the cohort shall be limited in size, specify positive
                integer

        :return: df containing the mapped or unmapped values from the db
        """
        data = self._data
        if included_mrns:
            data = data[data.medical_record_number.isin(included_mrns)]
        return data[:limit]

    def to_dict(self):
        """
        :return: a dict representation of this condition
        """
        return {
            'class': self.__class__.__name__,
            'attributes': {
                'data': [tuple(r) for r in self._data.values]
            }
        }

    @classmethod
    def from_dict(cls, json):
        """
        Args:
            json: the stored dict representation of this condition

        :return: the condition built from the json-dict
        """
        data = json['attributes']['data']
        df = pd.DataFrame(data, columns=OCCURRENCE_INDEX)
        return cls(df)
