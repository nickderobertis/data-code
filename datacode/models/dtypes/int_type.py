from typing import Type
import re

import numpy as np

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.bit_size import get_bit_from_dtype


class IntType(DataType):

    def __init__(self, categorical: bool = False, ordered: bool = False, bit_size: int = 64):
        self.bit_size = bit_size
        super().__init__(
            int,
            pd_class=self._get_pd_class(),
            names=('int', 'integer', 'int_', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'),
            categorical=categorical,
            ordered=ordered,
        )

    def _get_pd_class(self) -> Type:
        if self.bit_size == 8:
            return np.int8
        elif self.bit_size == 16:
            return np.int16
        elif self.bit_size == 32:
            return np.int32
        elif self.bit_size == 64:
            return np.int64
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
