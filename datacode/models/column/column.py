from typing import Optional, Sequence, Union

from datacode.models.column.index import ColumnIndex
from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables import Variable


class Column:

    def __init__(self, variable: Variable, indices: Optional[Sequence[ColumnIndex]] = None,
                 applied_transform_keys: Optional[Sequence[str]] = None,
                 dtype: Optional[Union[str, DataType]] = None):
        if applied_transform_keys is None:
            applied_transform_keys = []

        dtype = convert_str_to_data_type_if_necessary(dtype)

        if dtype is None:
            dtype = variable.dtype

        self.variable = variable
        self.indices = indices
        self.applied_transform_keys = applied_transform_keys
        self.dtype = dtype
