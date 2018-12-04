import pandas as pd

def diff_df_from_panel_df(df: pd.DataFrame, entity_var: str, time_var: str, treated_var: str,
                          treated_time_var: str = 'After') -> pd.DataFrame:
    df = df.sort_values([entity_var, time_var])
    first_df = df.groupby(entity_var, as_index=False).first()
    first_df[treated_time_var] = 0
    last_df = df.groupby(entity_var, as_index=False).last()
    last_df[treated_time_var] = 1
    diff_df = first_df.append(last_df)

    return diff_df