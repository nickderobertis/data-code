from copy import deepcopy
from typing import TYPE_CHECKING

from sympy import Symbol

from datacode.models.variables.transform.transform import AppliedTransform, Transform

if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column
    from datacode.models.source import DataSource


def create_changes_name_func(name: str, **kwargs) -> str:
    return name + ' Change'


def create_changes_symbol_func(sym: Symbol, **kwargs) -> Symbol:
    sym_str = str(sym)
    new_sym_str = r'\delta ' + sym_str
    sym = Symbol(new_sym_str)
    return sym


def create_changes_data_func(col: 'Column', variable: 'Variable', source: 'DataSource', **kwargs) -> 'DataSource':
    from datacode.models.variables.transform.specific.lag import lag_transform
    from datacode.models.source import DataSource

    # Extract index columns
    index_vars = []
    if col.indices:
        for col_idx in col.indices:
            index_vars.extend(col_idx.variables)
    index_cols = []
    for var in index_vars:
        index_cols.append(source.col_for(var))
    all_names = [variable.name] + [var.name for var in index_vars]

    applied_lag_transform = AppliedTransform.from_transform(lag_transform, **kwargs)
    lag_variable = deepcopy(variable)
    temp_col = deepcopy(col)
    temp_col.variable = lag_variable
    source_for_lag = DataSource(
        df=source.df[all_names],
        load_variables=[lag_variable] + index_vars,
        columns=[temp_col] + index_cols
    )
    source_with_lag = applied_lag_transform.apply_to_source(
        source_for_lag,
        preserve_original=False,
        subset=[lag_variable]
    )
    source.df[variable.name] = source.df[variable.name] - source_with_lag.df[lag_variable.name]

    return source


change_transform = Transform(
    'change',
    name_func=create_changes_name_func,
    data_func=create_changes_data_func,
    symbol_func=create_changes_symbol_func,
    data_func_target='source'
)
