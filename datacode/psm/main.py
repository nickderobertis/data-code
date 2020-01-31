import pandas as pd

from datacode.psm.predict import predict_probability_of_treatment
from datacode.psm.match import create_matched_df_using_propensity_scores
from datacode.psm.summarize.main import create_matching_summary_df_dict, create_and_output_matching_summary_latex_table
from datacode.psm.dataprep import treated_and_control_df_from_df
from datacode.psm.names import get_prob_treated_varname
from datacode.psm.typing import StrList, StrListOrNone, DfSummaryTuple, StrOrNone, FloatOrNone, DfDict, TwoDfTuple


def create_matched_df_and_summary(df: pd.DataFrame, treated_var: str, xvars: StrList,
                                  entity_var: str, time_var: str,
                                  fe: StrListOrNone = None,
                                  prob_treated_var_name: StrOrNone = None,
                                  min_matching_pct: FloatOrNone = None,
                                  control_only: bool = False,
                                  num_matches_per_entity: int = 1,
                                  match_with_replacement: bool = False,
                                  control_name: str = 'Control',
                                  treated_name: str = 'Treated', below_text: StrOrNone = None,
                                  outfolder: str = '.', caption: str = 'Propensity Score Matching',
                                  summary_as_table: bool = False
                                  ) -> DfSummaryTuple:
    prob_treated_var_name = get_prob_treated_varname(treated_var, prob_treated_var_name=prob_treated_var_name)
    matched_df, predict_df = create_matched_and_predict_df(
        df,
        treated_var,
        xvars,
        entity_var,
        time_var,
        fe=fe,
        prob_treated_var_name=prob_treated_var_name,
        min_matching_pct=min_matching_pct,
        control_only=False,
        num_matches_per_entity=num_matches_per_entity,
        match_with_replacement=match_with_replacement
    )

    common_args = (
        df,
        matched_df,
        predict_df,
        treated_var,
        xvars,
        entity_var
    )

    common_kwargs = dict(
        fe=fe,
        prob_treated_var=prob_treated_var_name,
        control_name=control_name,
        treated_name=treated_name
    )

    if summary_as_table:
        common_args = common_args + (time_var,)
        common_kwargs.update(dict(
            below_text=below_text,
            outfolder=outfolder,
            caption=caption,
            min_matching_pct=min_matching_pct
        ))
        summary_func = create_and_output_matching_summary_latex_table
    else:
        summary_func = create_matching_summary_df_dict

    df_dict_or_table = summary_func(
        *common_args,
        **common_kwargs
    )

    if control_only:
        _, matched_df = treated_and_control_df_from_df(df, treated_var, entity_var)

    return matched_df, df_dict_or_table


def create_matched_and_predict_df(df: pd.DataFrame, treated_var: str, xvars: StrList,
                                  entity_var: str, time_var: str,
                                  fe: StrListOrNone = None,
                                  prob_treated_var_name: StrOrNone = None,
                                  min_matching_pct: FloatOrNone = None,
                                  control_only: bool = False,
                                  num_matches_per_entity: int = 1,
                                  match_with_replacement: bool = False
                                  ) -> TwoDfTuple:
    prob_treated_var_name = get_prob_treated_varname(treated_var, prob_treated_var_name=prob_treated_var_name)
    predict_df = predict_probability_of_treatment(
        df,
        treated_var,
        xvars,
        entity_var,
        time_var,
        fe=fe
    )

    df[prob_treated_var_name] = predict_df[prob_treated_var_name]

    matched_df = create_matched_df_using_propensity_scores(
        df,
        predict_df,
        treated_var,
        time_var=time_var,
        entity_var=entity_var,
        min_matching_pct=min_matching_pct,
        control_only=control_only,
        num_matches_per_entity=num_matches_per_entity,
        match_with_replacement=match_with_replacement
    )

    return matched_df, predict_df