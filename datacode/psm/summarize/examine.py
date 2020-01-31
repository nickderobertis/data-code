import pandas as pd
from datacode.psm.score import comparison_arrays_from_treated_and_control_df, comparison_score


def yield_matched_scores(treated_df: pd.DataFrame, control_df: pd.DataFrame, match_dict: dict,
                         prob_treated_var: str, time_var: str = 'Date',
                         entity_var: str = 'id') -> pd.DataFrame:
    for treated_id, control_id in match_dict.items():
        arrays = comparison_arrays_from_treated_and_control_df(
            treated_df[treated_df[entity_var] == treated_id],
            control_df[control_df[entity_var] == control_id],
            prob_treated_var,
            time_var=time_var
        )
        df = pd.DataFrame([
            pd.Series(arr, name=name) for name, arr in
            {f'Treated: {treated_id}': arrays[0], f'Control: {control_id}': arrays[1]}.items()
        ]).T
        score = comparison_score(arrays)
        print(f'Score: {score}')
        yield df
