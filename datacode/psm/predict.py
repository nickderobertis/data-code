import pandas as pd
import statsmodels.api as sm
from regtools.iter import reg_for_each_xvar_set_and_produce_summary
from datacode.psm.typing import StrList, StrListOrNone, RegResultSummaryTuple
from datacode.psm.names import prob_treated_varname, predict_treated_varname, accurate_prediction_name


def explain_probability_of_treatment(df: pd.DataFrame, treated_var: str, xvars: StrList, fe: StrListOrNone = None
                                     ) -> RegResultSummaryTuple:
    reg_list, summ = reg_for_each_xvar_set_and_produce_summary(
        df,
        treated_var,
        [xvars],
        stderr=True,
        fe=fe,
        model_type='logit'
    )

    return reg_list[0], summ

def predict_probability_of_treatment(df: pd.DataFrame, treated_var: str, xvars: StrList,
                                     entity_var: str, time_var: str,
                                     fe: StrListOrNone = None
                                     ) -> pd.DataFrame:

    result, summ = explain_probability_of_treatment(
        df,
        treated_var,
        xvars,
        fe=fe
    )

    X = sm.add_constant(df[xvars])

    prob_treated_var = prob_treated_varname(treated_var)
    predict_treated_var = predict_treated_varname(treated_var)

    predict_df = df[[entity_var, time_var, treated_var]]
    predict_df[prob_treated_var] = result.predict(X)
    predict_df[predict_treated_var] = predict_df[prob_treated_var].apply(lambda x: 1 if x >= 0.5 else 0)
    predict_df[accurate_prediction_name] = predict_df[predict_treated_var] == predict_df[treated_var]

    return predict_df
