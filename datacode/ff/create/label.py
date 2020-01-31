import pandas as pd

from typing import List
from datacode.ff.fftypes import StrOrInt, DictofStrsandStrLists
from datacode.ff.create.model import parse_model

def get_and_set_labels(df: pd.DataFrame, factor_model: StrOrInt=3, size_var: str=None, value_var: str=None,
               profitability_var: str=None, investment_var: str=None,
               custom_labels: DictofStrsandStrLists=None) -> DictofStrsandStrLists:

    labels = _get_labels(
        factor_model=factor_model,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_labels=custom_labels
    )

    _set_labels(df, labels)

    return labels

def _set_labels(df: pd.DataFrame, labels: DictofStrsandStrLists):
    for col, labels in labels.items():
        _set_label(df, col, labels)

def _set_label(df: pd.DataFrame, label_var: str, labels: List[str]) -> None:
    """
    Note: inplace
    """
    assert len(labels) == len(df[label_var].unique())

    for i, label_value in enumerate(labels):
        port_num = i + 1 # portfolios are 1-indexed, not 0-indexed. portfolio 1 is lowest value portfolio
        df.loc[df[label_var] == port_num, label_var] = label_value

def _get_labels(factor_model: StrOrInt=3, size_var: str=None, value_var: str=None,
               profitability_var: str=None, investment_var: str=None,
               custom_labels: DictofStrsandStrLists=None) -> DictofStrsandStrLists:
    """
    Note: inplace
    """
    factor_model = parse_model(factor_model)

    if factor_model == 'custom':
        labels = custom_labels
    else:
        labels = _get_default_labels(
            size_var=size_var,
            value_var=value_var,
            profitability_var=profitability_var,
            investment_var=investment_var
        )

    return labels


def _get_default_labels(size_var: str, value_var: str,
                        profitability_var: str=None, investment_var: str=None) -> DictofStrsandStrLists:
    default_labels = {
        size_var: ['S','B'],
        value_var: ['L','M','H']
    }

    if profitability_var is not None:
        default_labels.update({
            profitability_var: ['W','M','R'],
        })

    if investment_var is not None:
        default_labels.update({
            investment_var: ['C','M','A'],
        })

    return default_labels