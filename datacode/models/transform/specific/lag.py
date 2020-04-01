import re
from typing import Tuple, TYPE_CHECKING, List

import pandas as pd
from sympy import Symbol

from datacode.models.transform.transform import Transform
from pd_utils.filldata import add_missing_group_rows

from datacode.models.dtypes.datetime_type import DatetimeType

if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column
    from datacode.models.source import DataSource

LAG_NAME_PATTERN = re.compile(r'(.+)\$_{t - (\d+)}\$')
LAG_SYMBOL_PATTERN = re.compile(r'(.+)_{t - (\d+)}')


def varname_to_lagged_varname(varname: str, num_lags: int = 1) -> str:
    if num_lags == 0:
        # No lag string necessary
        return varname

    try:
        base_var, existing_lags = lag_varname_to_varname_and_lag(varname)
    except VariableIsNotLaggedVariableException:
        # Variable is not already lagged, so just add lag portion to str
        return _varname_to_lagged_varname(varname, num_lags)

    # Variable was lagged originally, need to add an additional number of lags and apply to base name
    total_lags = existing_lags + num_lags
    return _varname_to_lagged_varname(base_var, total_lags)


def _varname_to_lagged_varname(varname: str, num_lags: int = 1) -> str:
    return varname + f"$_{{t - {num_lags}}}$"


def lag_varname_to_varname_and_lag(varname: str) -> Tuple[str, int]:
    match = LAG_NAME_PATTERN.match(varname)
    if match is None:
        raise VariableIsNotLaggedVariableException(f'could not parse {varname} as a lagged name')

    base_varname = match.group(1)
    lag_num = int(match.group(2))
    return base_varname, lag_num


def var_symbol_to_lagged_var_symbol(varname: str, num_lags: int = 1) -> str:
    if num_lags == 0:
        # No lag string necessary
        return varname

    try:
        base_var, existing_lags = lag_var_symbol_to_var_symbol_and_lag(varname)
    except VariableIsNotLaggedVariableException:
        # Variable is not already lagged, so just add lag portion to str
        return _var_symbol_to_lagged_var_symbol(varname, num_lags)

    # Variable was lagged originally, need to add an additional number of lags and apply to base name
    total_lags = existing_lags + num_lags
    return _var_symbol_to_lagged_var_symbol(base_var, total_lags)


def _var_symbol_to_lagged_var_symbol(varname: str, num_lags: int = 1) -> str:
    return varname + f"_{{t - {num_lags}}}"


def lag_var_symbol_to_var_symbol_and_lag(varname: str) -> Tuple[str, int]:
    match = LAG_SYMBOL_PATTERN.match(varname)
    if match is None:
        raise VariableIsNotLaggedVariableException(f'could not parse {varname} as a lagged name')

    base_varname = match.group(1)
    lag_num = int(match.group(2))
    return base_varname, lag_num


def _create_lagged_variable_panel(df: pd.DataFrame, col: str, by_vars: List[str], num_lags: int = 1):
    """
    Note: inplace
    """
    df[col] = df.groupby(by_vars)[col].shift(num_lags)


def _create_lagged_variable(df: pd.DataFrame, col: str, num_lags: int = 1) -> None:
    """
    Note: inplace
    """
    df[col] = df[col].shift(num_lags)


class VariableIsNotLaggedVariableException(Exception):
    pass


class DidNotFindSingleTimeIndexException(Exception):
    pass


def create_lags_transform_name_func(name: str, num_lags: int = 1, **kwargs) -> str:
    return varname_to_lagged_varname(name, num_lags=num_lags)


def create_lags_transform_symbol_func(sym: Symbol, num_lags: int = 1, **kwargs) -> str:
    sym_str = str(sym)
    new_sym_str = var_symbol_to_lagged_var_symbol(sym_str, num_lags=num_lags)
    return Symbol(new_sym_str)


def create_lags_transform_data_func(col: 'Column', variable: 'Variable', source: 'DataSource',
                                    num_lags: int = 1, fill_missing_rows: bool = True,
                                    fill_method="ffill", fill_limit: int = None) -> 'DataSource':
    orig_cols = list(source.df.columns)  # copy column order for reordering at end

    can_fill_missing_rows = False
    by_vars = []
    time_vars = []
    if col.indices:
        # Try to get time index
        time_indices = [
            col_idx for col_idx in col.indices if all(var.dtype == DatetimeType() for var in col_idx.variables)
        ]
        if len(time_indices) > 1:
            raise DidNotFindSingleTimeIndexException(f'found time indices {time_indices}, not clear how to lag')
        elif len(time_indices) != 0:
            # Got a time index, so can fill missing rows in that index,
            # so long as there is an id index (determined next)
            can_fill_missing_rows = True
        for col_idx in time_indices:
            time_vars.extend(col_idx.variables)

        # Get other indices
        other_indices = [col_idx for col_idx in col.indices if col_idx not in time_indices]
        if len(other_indices) > 0:
            # Got other indices
            for col_idx in other_indices:
                by_vars.extend(col_idx.variables)
        else:
            # Not by any index, so no reason to fill missing rows
            can_fill_missing_rows = False

    by_var_names = [var.name for var in by_vars]
    time_var_names = [var.name for var in time_vars]
    lag_kwargs = dict(num_lags=num_lags)
    if by_vars:
        lag_kwargs.update(by_vars=by_var_names)
        lag_func = _create_lagged_variable_panel
        source.df.sort_values(by_var_names + time_var_names, inplace=True)

        if fill_missing_rows and can_fill_missing_rows:
            # Save original index, for outputting df of same shape
            orig_index_df = source.df[[]]

            # TODO [#61]: use implementation of add_missing_group_rows which does not require dropping and resetting index
            #
            # Need to wait for pd_utils to support it
            orig_index_names = orig_index_df.index.names
            source.df = add_missing_group_rows(
                source.df.reset_index(), by_var_names, time_var_names, fill_method=fill_method, fill_limit=fill_limit
            ).set_index(orig_index_names)
    else:
        lag_func = _create_lagged_variable  # type: ignore

    lag_func(source.df, variable.name, **lag_kwargs)

    if by_vars and fill_missing_rows and can_fill_missing_rows:
        # Don't want to expand size of df
        # For some reason, join doesn't work correctly. So reset indexes, merge, and set indexes back
        source.df = orig_index_df.reset_index().merge(
            source.df.reset_index(), how='left', on=orig_index_df.index.names
        ).set_index(orig_index_df.index.names)
        # Reorder back to original order of columns
        source.df = source.df[orig_cols]

    return source


lag_transform = Transform(
    'lag',
    name_func=create_lags_transform_name_func,
    data_func=create_lags_transform_data_func,
    symbol_func=create_lags_transform_symbol_func,
    data_func_target='source'
)