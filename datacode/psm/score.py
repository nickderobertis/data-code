from typing import List, Optional
import pandas as pd
import numpy as np


def merge_treated_and_control_calculate_comparison_score(treated_entity_df: pd.DataFrame,
                                                         control_entity_df: pd.DataFrame,
                                                         prob_treated_var: str,
                                                         time_var: str = 'Date',
                                                         minimum_length: Optional[int] = None) -> float:
    compare_arrays = comparison_arrays_from_treated_and_control_df(
        treated_entity_df,
        control_entity_df,
        prob_treated_var,
        time_var=time_var,
        minimum_length=minimum_length
    )
    return comparison_score(compare_arrays)


def comparison_arrays_from_treated_and_control_df(treated_entity_df: pd.DataFrame,
                                                  control_entity_df: pd.DataFrame,
                                                  prob_treated_var: str,
                                                  time_var: str = 'Date',
                                                  minimum_length: Optional[int] = None,
                                                  ) -> List[np.array]:
    merged = treated_entity_df.merge(control_entity_df, how='inner', on=[time_var])

    if minimum_length is not None:
        df_len = len(merged)
        if df_len < minimum_length:
            raise NotEnoughTimeObservationsException(f'only had {df_len} obs while {minimum_length} were required')

    compare_arrays = [
        merged[prob_treated_var + '_x'].values,
        merged[prob_treated_var + '_y'].values
    ]

    return compare_arrays


def comparison_score(compare_arrays: List[np.array]) -> float:
    _validate_compare_arrays(compare_arrays)
    squared_diffs = (compare_arrays[0] - compare_arrays[1]) ** 2
    return sum(squared_diffs) / len(squared_diffs)


def _validate_compare_arrays(compare_arrays: List[np.array]):
    if len(compare_arrays) != 2:
        raise ValueError('must pass two compare arrays')
    if len(compare_arrays[0]) != len(compare_arrays[1]):
        raise ValueError('passed arrays must be of the same size.')
    if len(compare_arrays[0]) == 0:
        raise EmptyArraysException('cannot produce comparison score from empty arrays')


class NotEnoughTimeObservationsException(Exception):
    pass


class EmptyArraysException(Exception):
    pass