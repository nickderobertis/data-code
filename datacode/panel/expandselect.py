import pandas as pd
from dateutil.relativedelta import relativedelta

from datacode.typing import StrList

def expand_entity_date_selections(full_df: pd.DataFrame, selections_df: pd.DataFrame, cols: StrList = None,
                                  num_firms: int = 3, expand_months: int = 3,
                                  entity_id_col: str = 'TICKER',
                                  date_col: str = 'Date',
                                  begin_datevar: str = 'Begin Date', end_datevar: str = 'End Date',
                                  ) -> pd.DataFrame:

    entity_date_df = _firm_date_range_df_from_df(
        selections_df,
        num_firms=num_firms,
        firm_id_col=entity_id_col,
        date_col=date_col,
        begin_datevar=begin_datevar,
        end_datevar=end_datevar
    )
    _expand_date_df(
        entity_date_df,
        expand_months=expand_months,
        begin_datevar=begin_datevar,
        end_datevar=end_datevar
    )
    entity_df = _select_orig_df_from_date_df(
        full_df,
        entity_date_df,
        firm_id_col=entity_id_col,
        date_col=date_col,
        begin_datevar=begin_datevar,
        end_datevar=end_datevar,
        associated_cols=cols
    )

    return entity_df



def _firm_date_range_df_from_df(df: pd.DataFrame, num_firms: int = 3, firm_id_col: str = 'TICKER',
                                date_col: str = 'Date',
                                begin_datevar: str = 'Begin Date', end_datevar: str = 'End Date') -> pd.DataFrame:
    firm_date_vars = [firm_id_col, date_col]

    firms = df[firm_id_col].unique()[:num_firms]
    firm_dates = df[df[firm_id_col].isin(firms)][firm_date_vars].sort_values(firm_date_vars)
    earliest_dates = firm_dates.groupby(firm_id_col).min()
    earliest_dates.rename(columns={date_col: begin_datevar}, inplace=True)
    latest_dates = firm_dates.groupby(firm_id_col).max()
    latest_dates.rename(columns={date_col: end_datevar}, inplace=True)
    return earliest_dates.join(latest_dates)


def _expand_date_df(date_df: pd.DataFrame, expand_months: int = 3,
                    begin_datevar: str = 'Begin Date', end_datevar: str = 'End Date'):
    """
    Note: inplace
    """
    date_df[begin_datevar] = date_df[begin_datevar].apply(lambda x: x - relativedelta(months=expand_months))
    date_df[end_datevar] = date_df[end_datevar].apply(lambda x: x + relativedelta(months=expand_months))


def _select_orig_df_from_date_df(df: pd.DataFrame, date_df: pd.DataFrame,
                                 firm_id_col: str = 'TICKER',
                                 date_col: str = 'Date',
                                 begin_datevar: str = 'Begin Date', end_datevar: str = 'End Date',
                                 associated_cols: StrList = None) -> pd.DataFrame:
    if associated_cols == None:
        associated_cols = [firm_id_col, date_col]

    out_df = pd.DataFrame()
    for firm_id, date_series in date_df.iterrows():
        selected_firm_df = df.loc[
            (df[firm_id_col] == firm_id) &
            (df[date_col] >= date_series[begin_datevar]) &
            (df[date_col] <= date_series[end_datevar]),
            associated_cols
        ]
        out_df = out_df.append(selected_firm_df)

    return out_df