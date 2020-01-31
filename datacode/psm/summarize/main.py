import pandas as pd
from datacode.psm.typing import DfDict, StrList, StrOrNone, StrListOrNone
import pyexlatex.table as lt
from datacode.psm.predict import explain_probability_of_treatment
from datacode.psm.summarize.latex import matching_latex_table_from_df_dict
from datacode.psm.summarize.predictions import summarize_predictions
from datacode.psm.summarize.stats import matching_summary_stats
from datacode.psm.names import get_prob_treated_varname
from datacode.psm.typing import FloatOrNone

def create_and_output_matching_summary_latex_table(df: pd.DataFrame, matched_df: pd.DataFrame, predict_df: pd.DataFrame,
                                                   treated_var: str, xvars: StrList, entity_var: str, time_var: str,
                                                   fe: StrListOrNone = None,
                                                   prob_treated_var: StrOrNone = None, control_name: str = 'Control',
                                                   treated_name: str = 'Treated', below_text: StrOrNone = None,
                                                   outfolder: str = '.', caption: str = 'Propensity Score Matching',
                                                   min_matching_pct: FloatOrNone = None,
                                                   ) -> lt.Table:
    summary_df_dict = create_matching_summary_df_dict(
        df,
        matched_df,
        predict_df,
        treated_var,
        xvars,
        entity_var,
        fe=fe,
        prob_treated_var=prob_treated_var,
        control_name=control_name,
        treated_name=treated_name
    )

    table = matching_latex_table_from_df_dict(
        summary_df_dict,
        entity_var,
        time_var,
        below_text=below_text,
        caption=caption,
        min_matching_pct=min_matching_pct
    )

    table.to_pdf_and_move(
        outfolder=outfolder,
        outname=caption
    )

    return table


def create_matching_summary_df_dict(df: pd.DataFrame, matched_df: pd.DataFrame, predict_df: pd.DataFrame,
                                    treated_var: str, xvars: StrList, entity_var: str, fe: StrListOrNone = None,
                                    prob_treated_var: StrOrNone = None, control_name: str = 'Control',
                                    treated_name: str = 'Treated') -> DfDict:
    prob_treated_var = get_prob_treated_varname(treated_var, prob_treated_var_name=prob_treated_var)
    _, model_summ = explain_probability_of_treatment(
        df,
        treated_var,
        xvars,
        fe=fe
    )

    predict_summ = summarize_predictions(
        predict_df,
        treated_var,
        treated_name=treated_name,
        control_name=control_name
    )

    describe_vars = [treated_var, prob_treated_var] + xvars
    match_summ = matching_summary_stats(
        df,
        matched_df,
        treated_var,
        describe_vars,
        entity_var,
        control_name=control_name,
        treated_name=treated_name
    )

    return {
        'model': model_summ.tables[0],
        'predict': predict_summ,
        'match': match_summ
    }