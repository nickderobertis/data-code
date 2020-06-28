from typing import Optional, Sequence, Union, List

import pandas as pd
from mixins import ReprMixin
from mixins.attrequals import EqOnAttrsMixin

from datacode.models.column.index import ColumnIndex
from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables import Variable


class Column(EqOnAttrsMixin, ReprMixin):
    equal_attrs = [
        'variable',
        'indices',
        'load_key',
        'applied_transform_keys',
        'dtype',
    ]
    repr_cols = ['variable', 'load_key', 'indices', 'applied_transform_keys', 'dtype']

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
