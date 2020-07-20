import pandas as pd
from typing import List

from pd_utils.utils import _to_list_if_str
from datacode.ff.fftypes import (
    DictofStrsandStrLists,
    TwoStrTuple,
    ListOrStr,
    StrBoolDict,
    StrOrInt
)
from datacode.ff.create.model import parse_model

def construct_minus_variables(df: pd.DataFrame, labels: DictofStrsandStrLists, pairing: TwoStrTuple,
                              factor_model: StrOrInt=3, byvars: ListOrStr = None, datevar='Date',
                              size_var: str=None, value_var: str=None, profitability_var: str=None,
                              investment_var: str=None,
                              custom_low_minus_high_dict: StrBoolDict=None) -> pd.DataFrame:

    byvars = _to_list_if_str(byvars)

    low_minus_high_dict = _get_low_minus_high_dict(
        factor_model=factor_model,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_low_minus_high_dict=custom_low_minus_high_dict
    )

    minus_vars = []
    for index, portvar in enumerate(pairing):
        minus_vars.append(
            _construct_minus_variable(
                df,
                labels=labels,
                portvar_index=index,
                portvar=portvar,
                byvars=byvars,
                datevar=datevar,
                low_minus_high=low_minus_high_dict[portvar]
            )
        )

    # Rename first factor to show it was calculated with second factor. E.g. SMB -> SMB_HML
    new_main_portname = _calculated_with_varname(minus_vars[0], minus_vars[1])
    df.rename(columns={minus_vars[0]: new_main_portname}, inplace=True)
    minus_vars = [new_main_portname] + minus_vars[1:]

    if byvars is not None:
        all_vars = byvars + [datevar] + minus_vars
    else:
        all_vars = [datevar] + minus_vars

    return df[all_vars]


def _get_low_minus_high_dict(factor_model: StrOrInt=3, size_var: str=None, value_var: str=None,
               profitability_var: str=None, investment_var: str=None,
               custom_low_minus_high_dict: StrBoolDict=None) -> StrBoolDict:

    factor_model = parse_model(factor_model)

    if factor_model == 'custom':
        return custom_low_minus_high_dict

    low_minus_high_dict = {
        size_var: True,
        value_var: False
    }

    if profitability_var is not None:
        low_minus_high_dict.update({
            profitability_var: False,
            investment_var: True
        })

    return low_minus_high_dict

def _construct_minus_variable(df: pd.DataFrame, labels: DictofStrsandStrLists, portvar_index=0,
                              portvar: str='Market Equity', byvars: ListOrStr=None, datevar='Date',
                              low_minus_high=False) -> str:

    long_label, short_label = _set_long_label_short_label(labels, portvar=portvar, low_minus_high=low_minus_high)
    long_cols = _get_port_cols_matching_label_at_index(
        df, port_label=long_label, port_index=portvar_index, byvars=byvars, datevar=datevar
    )
    short_cols = _get_port_cols_matching_label_at_index(
        df, port_label=short_label, port_index=portvar_index, byvars=byvars, datevar=datevar
    )

    minus_label = _minus_label(long_label, short_label)
    df[minus_label] = df[long_cols].apply(lambda x: x.mean(), axis=1) - df[short_cols].apply(lambda x: x.mean(), axis=1)

    return minus_label


def _set_long_label_short_label(labels: DictofStrsandStrLists, portvar: str, low_minus_high: bool=False) -> TwoStrTuple:
    portvar_labels = labels[portvar]

    if low_minus_high:
        # long bottom portfolio, short top portfolio
        long_label = portvar_labels[0]
        short_label = portvar_labels[-1]
    else:
        # short bottom portfolio, long top portfolio
        long_label = portvar_labels[-1]
        short_label = portvar_labels[0]

    return long_label, short_label

def _get_port_cols_matching_label_at_index(df: pd.DataFrame, port_label: str='S', port_index=0,
                                  byvars: ListOrStr=None, datevar='Date') -> List[str]:

    if byvars is None:
        byvars = []

    port_cols = [col for col in df.columns if col not in byvars + [datevar]]
    return [col for col in port_cols if col[port_index] == port_label]

def _calculated_with_varname(orig_var: str, calculated_with: str):
    return f'{orig_var}_{calculated_with}'

def _calculated_with_varname_to_orig_var(calc_with_varname: str) -> str:
    return calc_with_varname.split('_')[0]

def _get_minus_label(labels: DictofStrsandStrLists,
                 portvar: str='Market Equity', low_minus_high=False) -> str:
    long_label, short_label = _set_long_label_short_label(labels, portvar=portvar, low_minus_high=low_minus_high)
    return _minus_label(long_label, short_label)

def _minus_label(long_label: str, short_label: str) -> str:
    return f'{long_label}M{short_label}'