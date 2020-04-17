from typing import Optional, Sequence, Union, List

import pandas as pd

from datacode.models.column.index import ColumnIndex
from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables import Variable


class Column:

    def __init__(self, variable: Variable, load_key: Optional[str] = None,
                 indices: Optional[Sequence[ColumnIndex]] = None,
                 applied_transform_keys: Optional[Sequence[str]] = None,
                 dtype: Optional[Union[str, DataType]] = None,
                 series: Optional[pd.Series] = None):
        if applied_transform_keys is None:
            applied_transform_keys = []

        dtype = convert_str_to_data_type_if_necessary(dtype)

        if dtype is None:
            dtype = variable.dtype

        self.load_key = load_key
        self.variable = variable
        self.indices = indices
        self.applied_transform_keys = applied_transform_keys
        self.dtype = dtype
        self.series = series

    def __repr__(self):
        return f'<Column(variable={self.variable}, load_key={self.load_key}, indices={self.indices}, ' \
               f'applied_transform_keys={self.applied_transform_keys}, dtype={self.dtype}>'

    def __eq__(self, other):
        if not isinstance(other, Column):
            return False

        return all([
            self.variable == other.variable,
            self.indices == other.indices,
            self.load_key == other.load_key,
            self.applied_transform_keys == other.applied_transform_keys,
            self.dtype == other.dtype
        ])

    @property
    def index_vars(self) -> List[Variable]:
        if self.indices is None:
            return []

        index_vars = []
        for col_idx in self.indices:
            for var in col_idx.variables:
                if var not in index_vars:
                    index_vars.append(var)
        return index_vars
