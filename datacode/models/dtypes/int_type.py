from typing import Type, Union
import re

import numpy as np
import pandas as pd

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.bit_size import get_bit_from_dtype


class IntType(DataType):
    names = ('int', 'integer', 'int_', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64')

    def __init__(self, categorical: bool = False, ordered: bool = False, bit_size: int = 64):
        self.bit_size = bit_size
        super().__init__(
            int,
            pd_class=self._get_pd_class(),
            categorical=categorical,
            ordered=ordered,
            is_numeric=True
        )

    def _get_pd_class(self) -> Type:
        if self.bit_size == 8:
            return pd.Int8Dtype
        elif self.bit_size == 16:
            return pd.Int16Dtype
        elif self.bit_size == 32:
            return pd.Int32Dtype
        elif self.bit_size == 64:
            return pd.Int64Dtype
        else:
            raise ValueError(f'must pass bit_size of 8, 16, 32, or 64. Got {self.bit_size}')

    @property
    def read_file_arg(self) -> Union[Type, str]:
        """
        Must use string for nullable int type loading
        :return:
        """
        if self.bit_size == 8:
            return 'Int8'
        elif self.bit_size == 16:
            return 'Int16'
        elif self.bit_size == 32:
            return 'Int32'
        elif self.bit_size == 64:
            return 'Int64'
        else:
            raise ValueError(f'must pass bit_size of 8, 16, 32, or 64. Got {self.bit_size}')

    @classmethod
    def from_str(cls, dtype: str, categorical: bool = False, ordered: bool = False):
        dtype = dtype.lower()
        if dtype not in cls.names:
            raise ValueError(f'Dtype {dtype} does not match valid names for {cls.__name__}: {cls.names}')
        bit_or_none = get_bit_from_dtype(dtype)
        if bit_or_none is None:
            bit_or_none = 64
        return cls(
            categorical=categorical,
            ordered=ordered,
            bit_size=bit_or_none
        )

    @property
    def index_arg(self) -> Union[Type, str]:
        return pd.Int64Index
