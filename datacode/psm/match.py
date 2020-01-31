from typing import Optional
import warnings
import math
import pandas as pd
from datacode.psm.score import (
    merge_treated_and_control_calculate_comparison_score,
    EmptyArraysException,
    NotEnoughTimeObservationsException
)
from datacode.psm.dataprep import treated_and_control_df_from_df
from datacode.psm.names import prob_treated_varname
from datacode.psm.typing import StrOrNone, FloatOrNone

def create_matched_df_using_propensity_scores(df: pd.DataFrame,
                                              predict_df: pd.DataFrame,
                                              treated_var: str,
                                              prob_treated_var: StrOrNone = None,
                                              time_var: str = 'Date',
                                              entity_var: str = 'id',
                                              min_matching_pct: FloatOrNone = None,
                                              control_only: bool = False,
                                              num_matches_per_entity: int = 1,
                                              match_with_replacement: bool = False
                                              ) -> pd.DataFrame:
    if prob_treated_var is None:
        prob_treated_var = prob_treated_varname(treated_var)

    treated_df, control_df = treated_and_control_df_from_df(
        predict_df,
        treated_var,
        entity_var,
        keep_vars=[prob_treated_var, entity_var, time_var]
    )

    match_dict = create_matched_id_dict_using_propensity_scores(
        treated_df,
        control_df,
        prob_treated_var,
        time_var=time_var,
        entity_var=entity_var,
        min_matching_pct=min_matching_pct,
        num_matches_per_entity=num_matches_per_entity,
        match_with_replacement=match_with_replacement
    )

    matched_df = create_matched_df_from_match_dict(
        df,
        match_dict,
        entity_var=entity_var,
        control_only=control_only
    )

    return matched_df

def create_matched_df_from_match_dict(df: pd.DataFrame, match_dict: dict, entity_var: str = 'id',
                                      control_only: bool = False) -> pd.DataFrame:
    control_firms_lol = list(match_dict.values())  # list of lists of control firms
    selected_firms = _flatten(control_firms_lol)
    if not control_only:
        selected_treated_firms = list(match_dict.keys())
        selected_firms.extend(selected_treated_firms)  # treated firms

    return df[df[entity_var].isin(selected_firms)]


def create_matched_id_dict_using_propensity_scores(treated_df: pd.DataFrame,
                                                   control_df: pd.DataFrame,
                                                   prob_treated_var: str,
                                                   time_var: str = 'Date',
                                                   entity_var: str = 'id',
                                                   min_matching_pct: FloatOrNone = None,
                                                   num_matches_per_entity: int = 1,
                                                   match_with_replacement: bool = False) -> dict:
    if min_matching_pct is None:
        min_matching_pct = 0

    match_dict = {entity_id: [] for entity_id in treated_df[entity_var].unique()}
    all_matched_firms = []
    for treated_entity_id, treated_entity_df in treated_df.groupby(entity_var):
        comparison_scores = {}
        min_matching_time = math.ceil(len(treated_entity_df) * min_matching_pct)
        for control_entity_id, control_entity_df in control_df.groupby(entity_var):
            if not match_with_replacement and control_entity_id in all_matched_firms:
                # Already matched this control firm, skip
                continue
            try:
                comparison_scores[control_entity_id] = merge_treated_and_control_calculate_comparison_score(
                    treated_entity_df,
                    control_entity_df,
                    prob_treated_var,
                    time_var=time_var,
                    minimum_length=min_matching_time if min_matching_pct != 0 else None
                )
            except EmptyArraysException:
                # No overlapping time observations, cannot be a match
                continue
            except NotEnoughTimeObservationsException:
                # Not enough overlapping time observations, cannot be a match
                continue
        if comparison_scores == {}:
            warnings.warn(f'Could not find another {entity_var} to pair with {treated_entity_id} that has'
                          f' {min_matching_time} overlapping {time_var} obs.')
            continue
        all_scores = list(comparison_scores.values())
        all_scores.sort()
        selected_scores = all_scores[:num_matches_per_entity]
        inverse_dict = {value: key for key, value in comparison_scores.items()}
        for score in selected_scores:
            match_firm = inverse_dict[score]
            match_dict[treated_entity_id].append(match_firm)
            all_matched_firms.append(match_firm)

    print(f'Found {len(all_matched_firms)} matches for {len(match_dict.keys())} entities.')

    return match_dict

def _flatten(list_) -> list:
    return [item for sublist in list_ for item in sublist]