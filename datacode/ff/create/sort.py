import pandas as pd
import pd_utils

from pd_utils.pdutils import _to_list_if_str
from datacode.ff.create.model import parse_model
from datacode.ff.fftypes import ListOrStr, DfListListStrTuple, GroupvarNgroupsDict, StrOrInt

def create_ff_portfolios(df: pd.DataFrame, factor_model: StrOrInt=3, byvars: ListOrStr='Year',
                         id_var: str='Ticker', date_var: str='Date',
                         size_var: str=None, value_var: str=None,
                         profitability_var: str=None, investment_var: str=None,
                         custom_groupvar_ngroups_dict: GroupvarNgroupsDict=None,
                         **portfolio_kwargs) -> pd.DataFrame:

    groupvar_ngroups_dict = _get_groupvars_ngroups_dict(
        factor_model=factor_model,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_groupvars_ngroups_dict=custom_groupvar_ngroups_dict
    )

    return create_portfolios(
        df,
        groupvar_ngroups_dict=groupvar_ngroups_dict,
        byvars=byvars,
        id_var=id_var,
        date_var=date_var,
        **portfolio_kwargs
    )




def create_portfolios(df: pd.DataFrame, groupvar_ngroups_dict: GroupvarNgroupsDict=None, byvars: ListOrStr='Year',
                      id_var: str='Ticker', date_var: str='Date',
                       **portfolio_kwargs) -> pd.DataFrame:

    if groupvar_ngroups_dict is None:
        groupvar_ngroups_dict = {'Market Equity': 2}

    for groupvar, ngroups in groupvar_ngroups_dict.items():
        df = create_portfolio(
            df,
            groupvar=groupvar,
            ngroups=ngroups,
            byvars=byvars,
            id_var=id_var,
            date_var=date_var,
            **portfolio_kwargs
        )

    return df

def create_portfolio(df: pd.DataFrame, groupvar: str='Market Equity', ngroups=2, byvars: ListOrStr='Year',
                    id_var: str='Ticker', date_var: str='Date',
                       **portfolio_kwargs) -> pd.DataFrame:
    portname = _other_groupvar_portname(groupvar)
    ports = _create_portfolio(df, ngroups=ngroups, groupvar=groupvar, byvars=byvars,
                               portvar=portname, id_var=id_var, date_var=date_var, **portfolio_kwargs)

    return ports

def _get_groupvars_ngroups_dict(factor_model: StrOrInt=3,
                                size_var: str=None, value_var: str=None,
                                profitability_var: str=None, investment_var: str=None,
                                custom_groupvars_ngroups_dict: GroupvarNgroupsDict=None) -> GroupvarNgroupsDict:

    factor_model = parse_model(factor_model)

    if factor_model == 'custom':
        return custom_groupvars_ngroups_dict

    groupvars_ngroups_dict = {
        size_var: 2,
        value_var: 3
    }

    if profitability_var is not None:
        groupvars_ngroups_dict.update({
            profitability_var: 3
        })

    if investment_var is not None:
        groupvars_ngroups_dict.update({
            investment_var: 3
        })

    return groupvars_ngroups_dict


def _create_portfolio(df: pd.DataFrame, groupvar: str='Market Equity', ngroups=2, byvars: ListOrStr='Year',
                       portvar: str='portfolio', id_var: str='Ticker', date_var: str='Date',
                       **portfolio_kwargs) -> pd.DataFrame:

    df_for_port_sort, byvars, result_byvars, port_datevar =_create_df_for_port_sort_byvars_result_byvars_port_datevar(
        df,
        groupvar=groupvar,
        byvars=byvars,
        portvar=portvar,
        id_var=id_var,
        date_var=date_var
    )

    # create one portfolio per byvars per id_var
    unique_ports = pd_utils.portfolio(
        df_for_port_sort.dropna(subset=[groupvar]),
        groupvar,
        ngroups=ngroups,
        byvars=byvars,
        portvar=portvar,
        **portfolio_kwargs
    )

    # Merge back to original dataframe
    ports = df.merge(unique_ports[result_byvars + [portvar, port_datevar]], how='left', on=result_byvars)

    return ports


def _create_df_for_port_sort_byvars_result_byvars_port_datevar(df: pd.DataFrame, groupvar: str='Market Equity',
                                                               byvars: ListOrStr='Year', portvar: str='portfolio',
                                                               id_var: str='Ticker',
                                                               date_var: str='Date') -> DfListListStrTuple:

    byvars = _to_list_if_str(byvars)
    result_byvars = byvars + [id_var]

    # create dataframe which has only the beginning of period values for each company
    df_for_port_sort = df.sort_values([id_var, date_var]).groupby(
        result_byvars, as_index=False)[result_byvars + [groupvar, date_var]].first()

    # Rename date variable
    port_datevar = portvar + ' Formation Date'
    assert port_datevar not in df.columns  # don't overwrite existing
    df_for_port_sort.rename(columns={date_var: port_datevar}, inplace=True)


    return df_for_port_sort, byvars, result_byvars, port_datevar


def _other_groupvar_portname(other_groupvar):
    return f'{other_groupvar} Portfolio'