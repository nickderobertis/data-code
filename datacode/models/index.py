from typing import Optional, Union

from mixins import ReprMixin

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary


class Index(ReprMixin):
    repr_cols = ['key', 'dtype']

    def __init__(self, key: str, dtype: Optional[Union[str, DataType]] = None):
        dtype = convert_str_to_data_type_if_necessary(dtype)

        self.key = key
        self.dtype = dtype

    def __eq__(self, other):
        if not isinstance(other, Index):
            return False

        return all([
            self.key == other.key,
            self.dtype == other.dtype
        ])
