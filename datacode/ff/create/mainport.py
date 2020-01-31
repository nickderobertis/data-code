
# For FF procedure, size is the main port

import pandas as pd

from datacode.ff.create.minus import (
    _get_minus_label,
    _get_low_minus_high_dict,
    _calculated_with_varname_to_orig_var
)
from datacode.ff.fftypes import (
    TwoStrTupleList,
    TwoStrListTuple,
    DictofStrsandStrLists,
    StrOrInt,
    StrBoolDict,
    Tuple,
    StrList
)

def combine_main_portfolios(df: pd.DataFrame, labels: DictofStrsandStrLists, pairings: TwoStrTupleList,
                            factor_model: StrOrInt=3,
                            size_var: str=None, value_var: str=None, profitability_var: str=None,
                            investment_var: str=None,
                            custom_low_minus_high_dict: StrBoolDict=None) -> None:
    """
    Note: inplace

    Args:
        df:
        labels:
        pairings:
        factor_model:
        byvars:
        size_var:
        value_var:
        profitability_var:
        investment_var:
        custom_low_minus_high_dict:

    Returns:

    """

    low_minus_high_dict = _get_low_minus_high_dict(
        factor_model=factor_model,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_low_minus_high_dict=custom_low_minus_high_dict
    )

    # Split processing into repeated (need to be averaged) and unique (need to be renamed)
    repeated_main_ports, unique_main_ports = _get_repeated_and_unique_primary_portvars(pairings)
    _combine_repeated_ports(
        df=df,
        labels=labels,
        repeated_ports=repeated_main_ports,
        low_minus_high_dict=low_minus_high_dict
    )
    _rename_unique_ports(
        df=df,
        labels=labels,
        unique_ports=unique_main_ports,
        low_minus_high_dict=low_minus_high_dict
    )


def _combine_repeated_ports(df: pd.DataFrame, labels: DictofStrsandStrLists, repeated_ports: StrList,
                            low_minus_high_dict: StrBoolDict) -> None:
    for portvar in repeated_ports:
        low_minus_high = low_minus_high_dict[portvar]
        _combine_repeated_port(
            df=df,
            labels=labels,
            portvar=portvar,
            low_minus_high=low_minus_high
        )


def _combine_repeated_port(df: pd.DataFrame, labels: DictofStrsandStrLists, portvar: str='Market Equity', low_minus_high=False) -> None:
    """
    Note: inplace

    Args:
        df:
        labels:
        portvar:
        low_minus_high:

    Returns:

    """
    label, matching_ports = _get_label_and_matching_ports(
        df=df,
        labels=labels,
        portvar=portvar,
        low_minus_high=low_minus_high
    )

    df[label] = df[matching_ports].apply(lambda x: x.mean(), axis=1)
    df.drop(matching_ports, axis=1, inplace=True)


def _rename_unique_ports(df: pd.DataFrame, labels: DictofStrsandStrLists, unique_ports: StrList,
                            low_minus_high_dict: StrBoolDict) -> None:
    for portvar in unique_ports:
        low_minus_high = low_minus_high_dict[portvar]
        _rename_unique_port(
            df=df,
            labels=labels,
            portvar=portvar,
            low_minus_high=low_minus_high
        )


def _rename_unique_port(df: pd.DataFrame, labels: DictofStrsandStrLists, portvar: str='Market Equity', low_minus_high=False) -> None:
    """
    Note: inplace

    Args:
        df:
        labels:
        portvar:
        low_minus_high:

    Returns:

    """
    label, matching_ports = _get_label_and_matching_ports(
        df=df,
        labels=labels,
        portvar=portvar,
        low_minus_high=low_minus_high
    )

    if len(matching_ports) > 1:
        raise ValueError(f'got portvar {label} as unique, expected only one column match. '
                         f'got {len(matching_ports)} matches: {matching_ports}')

    df.rename(columns={matching_ports[0]: label}, inplace=True)


def _get_label_and_matching_ports(df: pd.DataFrame, labels: DictofStrsandStrLists, portvar: str='Market Equity', low_minus_high=False) -> Tuple[str, StrList]:
    label = _get_minus_label(
        labels=labels,
        portvar=portvar,
        low_minus_high=low_minus_high
    )

    matching_ports = [col for col in df.columns if _calculated_with_varname_to_orig_var(col) == label]

    return label, matching_ports

def _get_repeated_and_unique_primary_portvars(pairings: TwoStrTupleList) -> TwoStrListTuple:
    unique_portvars = []
    repeated_portvars = []
    for main_portvar, secondary_portvar in pairings:
        if main_portvar in repeated_portvars:
            # Third or later time seeing variable, don't need to do anything
            continue
        elif main_portvar in unique_portvars:
            # Second time seeing variable, remove from unique, add to repeated
            repeated_portvars.append(main_portvar)
            unique_portvars.remove(main_portvar)
        else:
            # First time seeing variable, set to unique
            unique_portvars.append(main_portvar)

    return repeated_portvars, unique_portvars
