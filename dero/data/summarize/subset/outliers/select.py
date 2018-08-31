import pandas as pd
from dateutil.relativedelta import relativedelta

from dero.data.summarize.subset.outliers.typing import (
    StrList,
    AssociatedColDict,
    BoolDict,
    DfDict,
    TwoDfDictAndDfTuple,
    MinMaxDict
)


def outlier_summary_dicts(df: pd.DataFrame, associated_col_dict: AssociatedColDict,
                          min_max_dict: MinMaxDict,
                          ascending_sort_dict: BoolDict = None,
                          always_associated_cols: StrList = None, bad_column_name: str = '_bad_column',
                          num_firms: int = 3, firm_id_col: str = 'TICKER',
                          date_col: str = 'Date',
                          begin_datevar: str = 'Begin Date',
                          end_datevar: str = 'End Date',
                          expand_months: int = 3
                          ) -> TwoDfDictAndDfTuple:

    bad_df = _bad_df_from_df(
        df,
        min_max_dict,
        bad_column_name=bad_column_name
    )

    bad_df_dict = _column_bad_df_dict(
        bad_df,
        associated_col_dict,
        ascending_sort_dict=ascending_sort_dict,
        always_associated_cols=always_associated_cols,
        bad_column_name=bad_column_name
    )

    selected_orig_df_dict = _col_bad_df_dict_to_selected_orig_df_dict(
        df,
        bad_df_dict,
        associated_col_dict,
        always_associated_cols=always_associated_cols,
        num_firms=num_firms,
        firm_id_col=firm_id_col,
        date_col=date_col,
        begin_datevar=begin_datevar,
        end_datevar=end_datevar,
        expand_months=expand_months
    )

    return bad_df_dict, selected_orig_df_dict, bad_df


def drop_outliers_by_cutoffs(df: pd.DataFrame, min_max_dict: MinMaxDict):
    valid_df = df.copy()
    for col, min_max in min_max_dict.items():
        col_min = min_max[0]
        col_max = min_max[1]
        if col in df.columns:
            valid_df = valid_df.loc[
                (valid_df[col] >= col_min) & (valid_df[col] <= col_max)
            ]


    return valid_df


def _bad_df_from_df(df: pd.DataFrame, min_max_dict: MinMaxDict,
             bad_column_name: str = '_bad_column'):
    bad_df = pd.DataFrame()
    for col, min_max in min_max_dict.items():
        col_min = min_max[0]
        col_max = min_max[1]
        col_bad_df = df.loc[
            (df[col] < col_min) | (df[col] > col_max)
            ]
        col_bad_df[bad_column_name] = col
        bad_df = bad_df.append(
            col_bad_df
        )

    return bad_df

def _column_bad_df_dict(bad_df: pd.DataFrame, associated_col_dict: AssociatedColDict,
                        ascending_sort_dict: BoolDict = None,
                        always_associated_cols: StrList = None, bad_column_name: str = '_bad_column') -> DfDict:
    if always_associated_cols is None:
        always_associated_cols = []

    # Default is ascending=False for all variables
    if ascending_sort_dict is None:
        ascending_sort_dict = {col: False for col in associated_col_dict}

    df_dict = {}
    for col, associated_cols in associated_col_dict.items():
        df_dict[col] = _sorted_bad_df_for_column(
            bad_df,
            col,
            always_associated_cols + associated_cols,
            bad_column_name=bad_column_name,
            sort_ascending=ascending_sort_dict[col]
        )

    return df_dict


def _col_bad_df_dict_to_selected_orig_df_dict(df: pd.DataFrame, col_bad_df_dict: DfDict,
                                              associated_col_dict: AssociatedColDict,
                                              always_associated_cols: StrList = None,
                                              num_firms: int = 3, firm_id_col: str = 'TICKER',
                                              date_col: str = 'Date',
                                              begin_datevar: str = 'Begin Date',
                                              end_datevar: str = 'End Date',
                                              expand_months: int = 3) -> DfDict:
    df_dict = {}
    for col, col_bad_df in col_bad_df_dict.items():
        firm_date_df = _firm_date_range_df_from_df(
            col_bad_df,
            num_firms=num_firms,
            firm_id_col=firm_id_col,
            date_col=date_col,
            begin_datevar=begin_datevar,
            end_datevar=end_datevar
        )
        _expand_date_df(
            firm_date_df,
            expand_months=expand_months,
            begin_datevar=begin_datevar,
            end_datevar=end_datevar
        )
        firm_df = _select_orig_df_from_date_df(
            df,
            firm_date_df,
            col,
            firm_id_col=firm_id_col,
            date_col=date_col,
            begin_datevar=begin_datevar,
            end_datevar=end_datevar,
            associated_cols=always_associated_cols + associated_col_dict[col]
        )
        df_dict[col] = firm_df

    return df_dict


def _sorted_bad_df_for_column(bad_df: pd.DataFrame, col: str, associated_cols: StrList,
                              bad_column_name: str = '_bad_column', sort_ascending: bool = False) -> pd.DataFrame:
    return bad_df.loc[
        bad_df[bad_column_name] == col,
        associated_cols + [col]
    ].sort_values(col, ascending=sort_ascending)


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


def _select_orig_df_from_date_df(df: pd.DataFrame, date_df: pd.DataFrame, col: str,
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
            associated_cols + [col]
        ]
        out_df = out_df.append(selected_firm_df)

    return out_df