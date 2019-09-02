from typing import Iterable, Union

import pandas as pd

from fiber import OCCURRENCE_INDEX
from fiber.condition.base import BaseCondition


class MRNs(BaseCondition):
    def __init__(
        self,
        mrns: Union[pd.DataFrame, Iterable[str]] = None
    ):
        df = pd.DataFrame(columns=OCCURRENCE_INDEX)

        if isinstance(mrns, pd.DataFrame):
            df = df.append(mrns)
        elif isinstance(mrns, Iterable):
            df.medical_record_number = df.medical_record_number.append(
                pd.Series(mrns))

        self._data = df.sort_values(OCCURRENCE_INDEX)
        self._mrns = set(self._data.medical_record_number)

    def _fetch_data(self, included_mrns=None, limit=None):
        data = self._data
        if included_mrns:
            data = data[data.medical_record_number.isin(included_mrns)]
        return data[:limit]

    def to_dict(self):
        return {
            'class': self.__class__.__name__,
            'attributes': {
                'data': [tuple(r) for r in self._data.values]
            }
        }

    @classmethod
    def from_dict(cls, json):
        data = json['attributes']['data']
        df = pd.DataFrame(data, columns=OCCURRENCE_INDEX)
        return cls(df)
