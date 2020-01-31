import pandas as pd

from datacode.ff.create.model import parse_model
from datacode.ff.fftypes import StrOrInt, TwoStrTupleList, TwoStrTuple

def create_dual_sort_variables_get_pairings(df: pd.DataFrame, factor_model: StrOrInt, size_var: str=None, value_var: str=None,
                               profitability_var: str=None, investment_var: str=None,
                               custom_pairings: TwoStrTupleList=None) -> TwoStrTupleList:

    pairings = _select_pairings(
        factor_model=factor_model,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_pairings=custom_pairings
    )

    _create_dual_sort_variables(df, pairings)

    return pairings

def _create_dual_sort_variables(df: pd.DataFrame, parings: TwoStrTupleList) -> None:
    [_create_dual_sort_variable(df, pairing) for pairing in parings]

def _create_dual_sort_variable(df: pd.DataFrame, pairing: TwoStrTuple) -> None:
    combined_name = _dual_sort_varname(pairing[0], pairing[1])
    df[combined_name] = df[pairing[0]].astype(str) + df[pairing[1]].astype(str)


def _select_pairings(factor_model: StrOrInt, size_var: str=None, value_var: str=None,
                     profitability_var: str=None, investment_var: str=None,
                     custom_pairings: TwoStrTupleList=None) -> TwoStrTupleList:

    factor_model = parse_model(factor_model)

    if factor_model == 'custom':
        return custom_pairings

    if factor_model == 3:
        return [
            (size_var, value_var)
        ]

    if factor_model == 5:
        return [
            (size_var, value_var),
            (size_var, profitability_var),
            (size_var, investment_var)
        ]


def _dual_sort_varname(portvar1: str, portvar2: str) -> str:
    return f'{portvar1}/{portvar2}'