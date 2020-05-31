import datetime
from copy import deepcopy
from typing import Union, Optional, Type

import numpy as np

from datacode.models.dtypes.base import DataType


class DatetimeType(DataType):
    names = ('datetime', 'time', 'datetime64')

    def __init__(self, categorical: bool = False, ordered: bool = False,
                 tz: Optional[Union[str, datetime.timezone]] = None):
        super().__init__(
            datetime.datetime,
            pd_class=np.datetime64,
            categorical=categorical,
            ordered=ordered,
        )
        self.tz = tz
        self.equal_attrs = deepcopy(self.equal_attrs)
        self.equal_attrs.append('tz')

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
        return 'datetime64[ns]'
