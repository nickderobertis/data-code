from typing import Optional, Sequence, Union, List, Iterable, Dict, Any

import pandas as pd
from mixins import ReprMixin
from mixins.attrequals import EqOnAttrsMixin

from datacode.models.column.index import ColumnIndex
from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables import Variable
from datacode.models.transform.transform import Transform


class Column(EqOnAttrsMixin, ReprMixin):
    equal_attrs = [
        'variable',
        'indices',
        'load_key',
        'applied_transform_keys',
        'dtype',
    ]
    repr_cols = ['variable', 'load_key', 'indices', 'applied_transform_keys', 'dtype']
    copy_attrs = equal_attrs + ['series']

    def __init__(self, variable: Variable, load_key: Optional[str] = None,
                 indices: Optional[Sequence[ColumnIndex]] = None,
                 applied_transform_keys: Optional[Sequence[str]] = None,
                 dtype: Optional[Union[str, DataType]] = None,
                 series: Optional[pd.Series] = None):
        if applied_transform_keys is None:
            applied_transform_keys = []

        dtype = convert_str_to_data_type_if_necessary(dtype)

        if dtype is None and variable is not None:
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

    def _add_applied_transform(self, transform: Transform, skip_variable: bool = False):
        self.applied_transform_keys.append(transform.key)
        if not skip_variable:
            self.variable._add_applied_transform(transform)

    def copy(self, exclude_attrs: Iterable[str] = ('series',)):
        data: Dict[str, Any] = {}
        for attr in self.copy_attrs:
            if attr in exclude_attrs:
                continue
            data[attr] = getattr(self, attr)
        return self.__class__(**data)