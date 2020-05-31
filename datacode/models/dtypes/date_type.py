import datetime
from typing import Union, Type

import numpy as np
import pandas as pd

from datacode.models.dtypes.base import DataType


class DateType(DataType):
    names = ('date',)

    def __init__(self, categorical: bool = False, ordered: bool = False):
        super().__init__(
            datetime.date,
            pd_class=np.datetime64,
            categorical=categorical,
            ordered=ordered,
        )

    @classmethod
    def from_str(cls, dtype: str, categorical: bool = False, ordered: bool = False):
        dtype = dtype.lower()
        if dtype not in cls.names:
            raise ValueError(f'Dtype {dtype} does not match valid names for {cls.__name__}: {cls.names}')
        return cls(
            categorical=categorical,
            ordered=ordered
        )

    @property
    def index_arg(self) -> Union[Type, str]:
        return pd.DatetimeTZDtype
