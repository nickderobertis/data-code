from typing import Union, Type

import pandas as pd

from datacode.models.dtypes.base import DataType


class StringType(DataType):
    names = ('str', 'string', 'character', 'str_')

    def __init__(self, categorical: bool = False, ordered: bool = False):
        # TODO [#23]: add all string methods as available transforms
        super().__init__(
            str,
            pd_class=pd.StringDtype,
            categorical=categorical,
            ordered=ordered
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
    def read_file_arg(self) -> Union[Type, str]:
        if self.categorical:
            return 'category'
        return 'string'

    @property
    def index_arg(self) -> Union[Type, str]:
        return self.pd_class
