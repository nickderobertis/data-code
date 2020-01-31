from datacode.psm.typing import StrOrNone

accurate_prediction_name = 'Accurate Prediction'
diff_var = 'Diff'
t_stat_var = '$t$-stat'
matched_var = 'Matched'

accuracy_pct = '% Accuracy'

def get_prob_treated_varname(treated_var: str, prob_treated_var_name: StrOrNone = None) -> str:
    if prob_treated_var_name is None:
        return prob_treated_varname(treated_var)

    return prob_treated_var_name

def prob_treated_varname(treated_var: str) -> str:
    return f'Prob({treated_var})'

def predict_treated_varname(treated_var: str) -> str:
    return f'Predicted {treated_var}'