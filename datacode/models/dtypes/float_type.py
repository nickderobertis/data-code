from copy import deepcopy
from typing import Type

import numpy as np

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.bit_size import get_bit_from_dtype


class FloatType(DataType):
    names = ('float', 'decimal', 'float16', 'float32', 'float64', 'float128', 'floating')

    def __init__(self, categorical: bool = False, ordered: bool = False, bit_size: int = 64):
        self.bit_size = bit_size
        super().__init__(
            float,
            pd_class=self._get_pd_class(),
            categorical=categorical,
            ordered=ordered,
            is_numeric=True
        )
        self.equal_attrs = deepcopy(self.equal_attrs)
        self.equal_attrs.append('bit_size')
        self.repr_cols = deepcopy(self.repr_cols)
        self.repr_cols.append('bit_size')

    def _get_pd_class(self) -> Type:
        if self.bit_size == 16:
            return np.float16
        elif self.bit_size == 32:
            return np.float32
        elif self.bit_size == 64:
            return np.float64
        elif self.bit_size == 128:
            return np.float128
        else:
            raise ValueError(f'must pass bit_size of 16, 32, 64 or 128. Got {self.bit_size}')

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
