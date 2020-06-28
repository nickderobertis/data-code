import datetime
from copy import deepcopy
from typing import Union, Optional, Type

import pandas as pd

from datacode.models.dtypes.base import DataType


class PeriodType(DataType):
    name_roots = ('period',)

    def __init__(self, freq: str, categorical: bool = False, ordered: bool = False):
        super().__init__(
            datetime.datetime,
            pd_class=pd.PeriodDtype,
            categorical=categorical,
            ordered=ordered,
        )
        self.freq = freq
        self.equal_attrs = deepcopy(self.equal_attrs)
        self.equal_attrs.append('freq')
        self.repr_cols = deepcopy(self.repr_cols)
        self.repr_cols.append('freq')

    @classmethod
    def from_str(cls, dtype: str, categorical: bool = False, ordered: bool = False):
        dtype = dtype.lower()
        freq: Optional[str] = None
        for name in cls.name_roots:
            if dtype.startswith(name):
                _, freq_extra = dtype.split('[')
                freq = freq_extra.strip(']')
        if freq is None:
            raise ValueError(f'Dtype {dtype} does not match valid names for {cls.__name__}: {cls.names}')
        return cls(
            freq,
            categorical=categorical,
            ordered=ordered
        )

    @property
    def read_file_arg(self) -> Union[Type, str]:
        return f'period[{self.freq}]'
