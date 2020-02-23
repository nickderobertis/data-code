from typing import TYPE_CHECKING

from sympy import Symbol

from datacode.models.transform.transform import Transform
from datacode.models.transform.applied import AppliedTransform

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
    from datacode.models.transform.specific.lag import lag_transform

    applied_lag_transform = AppliedTransform.from_transform(lag_transform, **kwargs)
    source_for_lag = source.copy(
        df=source.df[[variable.name]]
    )
    source_with_lag = applied_lag_transform.apply_to_source(
        source_for_lag,
        preserve_original=False,
        subset=[variable]
    )
    source.df[variable.name] = source.df[variable.name] - source_with_lag.df[variable.lag(**kwargs).name]

    return source


change_transform = Transform(
    'change',
    name_func=create_changes_name_func,
    data_func=create_changes_data_func,
    symbol_func=create_changes_symbol_func,
    data_func_target='source'
)
