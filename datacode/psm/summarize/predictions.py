import pandas as pd
from functools import partial
from datacode.psm.names import predict_treated_varname, accurate_prediction_name, accuracy_pct
from datacode.summarize import format_numbers_to_decimal_places

def summarize_predictions(predict_df: pd.DataFrame, treated_var: str, treated_name: str = 'Treated',
                          control_name: str = 'Control') -> pd.DataFrame:
    predict_treated_var = predict_treated_varname(treated_var)

    treated_series = predict_df[[treated_var, predict_treated_var]].sum().astype(int)
    control_series = (len(predict_df) - treated_series).astype(int)
    correct_predictions_series = predict_df.groupby(treated_var)[accurate_prediction_name].sum().astype(int)
    count_series = pd.Series([control_series[treated_var], treated_series[treated_var]])

    predict_summ = pd.concat([control_series, treated_series], axis=1)
    predict_summ = pd.concat([
        predict_summ,  # num obs and predicted obs
        pd.DataFrame(correct_predictions_series).T,  # num accurate
        pd.DataFrame(correct_predictions_series / count_series).T  # pct accurate
    ])
    predict_summ.columns = [control_name, treated_name]
    predict_summ.index = ['# Obs', '# Predicted', '# Accurate', accuracy_pct]
    predict_summ = _format_predictions(predict_summ)

    return predict_summ[[treated_name, control_name]]

def _format_predictions(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    df.loc[accuracy_pct] = df.loc[accuracy_pct] * 100
    num_formatter = partial(format_numbers_to_decimal_places, decimals=decimals, coerce_ints=True)
    df = df.applymap(format_numbers_to_decimal_places)

    return df