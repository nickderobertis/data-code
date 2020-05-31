from copy import deepcopy
from typing import Optional, Sequence, TYPE_CHECKING, Union

from mixins.repr import ReprMixin
from sympy import Symbol

if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column
    from datacode.models.source import DataSource
    from datacode.models.dtypes.base import DataType

from datacode.models.logic.partial import partial
from datacode.models.variables.compare import functions_are_equal
from datacode.models.variables.typing import StrFunc, ValueFunc, SymbolFunc


class Transform(ReprMixin):
    """
    Tracks and applies changes to variables for data, name, and symbol together
    """
    repr_cols = ['key', 'name_func', 'data_func', 'symbol_func', 'data_func_target', 'new_dtype']

    def __init__(self, key: str, name_func: StrFunc = None, data_func: ValueFunc = None,
                 symbol_func: SymbolFunc = None,
                 data_func_target: str = 'series', new_dtype: Optional[Union['DataType', str]] = None):
        from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
        if new_dtype is not None:
            new_dtype = convert_str_to_data_type_if_necessary(new_dtype)

        # If name func passed but not symbol func, convert name func to symbol func
        if name_func is not None and symbol_func is None:
            def sym_func_from_name_func(sym: Symbol, *args, **kwargs) -> Symbol:
                symbol_str = str(sym)
                new_symbol_str = name_func(symbol_str, *args, **kwargs)
                new_symbol = Symbol(new_symbol_str)
                return new_symbol
            symbol_func = sym_func_from_name_func


        self.key = key
        self.name_func = name_func
        self.data_func = data_func
        self.symbol_func = symbol_func
        self.data_func_target = self._validate_data_func_target(data_func_target)
        self.new_dtype = new_dtype

    def __hash__(self):
        hash_attrs = ['key', 'data_func_target']
        hash_items = tuple([getattr(self, attr) for attr in hash_attrs])
        return hash(hash_items)

    def _validate_data_func_target(self, data_func_target: str):
        data_func_target = data_func_target.lower()
        cell_values = ('cell', 'value', 'item', 'c', 'v')
        series_values = ('series', 's')
        df_values = ('df', 'dataframe')
        source_values = ('ds', 'datasource', 'source')
        if data_func_target in cell_values:
            return 'cell'
        if data_func_target in series_values:
            return 'series'
        if data_func_target in df_values:
            return 'dataframe'
        if data_func_target in source_values:
            return 'source'
        raise ValueError(f'Did not pass appropriate data_func_target to Transform {self}, got {data_func_target} '
                         f'which should be one of cell, series, dataframe, or source')

    def apply_to_source(self, source: 'DataSource', preserve_original: bool = True,
                        subset: Optional[Sequence['Variable']] = None) -> 'DataSource':
        """
        Applies transformation to entire data source

        :param source:
        :param preserve_original: True to copy the source before applying transformations. False will decrease
        memory usage but will cause the original source to be partially modified
        :param subset: Variables to apply the transformation to. Defaults to all variables.
        :return:
        """
        if preserve_original:
            source = deepcopy(source)
        else:
            # Even when not preserving original, don't want to modify original variables or columns
            # as they may be used in other sources

            # NOTE: not working yet, see SourceTransform.apply
            source.load_variables = deepcopy(source.load_variables)
            source.columns = deepcopy(source.columns)

        if subset:
            variables = deepcopy(subset)
        else:
            variables = source.load_variables

        rename_dict = {}
        for selected_var in variables:
            col = source.col_for(var_key=selected_var.key)
            var = col.variable  # Don't use passed variable directly besides key as may be different instance

            # Apply data transformation
            source = self._apply_transform_for_column_and_variable_to_source(source, col, var)

            # Update variable
            orig_name = var.name
            var._add_applied_transform(self)
            new_name = var.name

            # Update column name
            if orig_name != new_name:
                rename_dict[orig_name] = new_name

        if rename_dict:
            source.df.rename(columns=rename_dict, inplace=True)

        return source

    def _apply_transform_for_column_and_variable_to_source(self, source: 'DataSource', column: 'Column',
                                                           variable: 'Variable') -> 'DataSource':
        source = self._apply_data_transform_to_column_and_variable_in_source(source, column, variable)
        return source

    def _apply_data_transform_to_column_and_variable_in_source(self, source: 'DataSource', column: 'Column',
                                                               variable: 'Variable') -> 'DataSource':
        if self.data_func is None:
            return source
        data_func_with_col = partial(self.data_func, column, variable)
        if self.data_func_target == 'cell':
            source.df[variable.name] = source.df[variable.name].apply(data_func_with_col)
        elif self.data_func_target == 'series':
            source.df[variable.name] = data_func_with_col(source.df[variable.name])
        elif self.data_func_target == 'dataframe':
            source.df = data_func_with_col(source.df)
        elif self.data_func_target == 'source':
            source = data_func_with_col(source)
        else:
            raise ValueError(f'Did not pass appropriate data_func_target to Transform {self}, got '
                             f'{self.data_func_target} which should be one of cell, series, dataframe, or source')

        if self.new_dtype is not None:
            dtype = self.new_dtype
        else:
            dtype = column.dtype
        dtype_str = dtype.read_file_arg
        source.df[variable.name] = source.df[variable.name].astype(dtype_str)

        column.series = source.df[variable.name]
        return source

    def __eq__(self, other):
        same = True
        try:
            same = same and self.key == other.key
            same = same and functions_are_equal(self.name_func, other.name_func)
            same = same and functions_are_equal(self.data_func, other.data_func)
            same = same and self.data_func_target == other.data_func_target
            return same
        except AttributeError:
            return False
