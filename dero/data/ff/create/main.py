import pandas as pd

from dero.ext_pandas.pdutils import _to_list_if_str
from dero.data.ff.fftypes import (
    ListOrStr,
    StrOrInt,
    DictofStrsandStrLists,
    GroupvarNgroupsDict,
    TwoStrTupleList,
    TwoStrTuple,
    StrBoolDict
)
from dero.data.ff.create.model import _validate_model
from dero.data.ff.create.sort import create_ff_portfolios, _other_groupvar_portname
from dero.data.ff.create.label import get_and_set_labels
from dero.data.ff.create.dualsort import create_dual_sort_variables_get_pairings, _dual_sort_varname
from dero.data.ff.create.average import portfolio_returns
from dero.data.ff.create.reshape import long_averages_to_wide_averages
from dero.data.ff.create.minus import construct_minus_variables
from dero.data.ff.create.inputs import _standardize_custom_args

def create_ff_factors(df: pd.DataFrame, factor_model: StrOrInt,
                      id_var: str='PERMNO', datevar='Date', byvars: ListOrStr=None,
                      retvar: str='RET', wtvar: str='Market Equity',
                      size_var: str = None, value_var: str = None,
                      profitability_var: str = None, investment_var: str = None,
                      custom_labels: DictofStrsandStrLists = None,
                      custom_groupvar_ngroups_dict: GroupvarNgroupsDict=None,
                      custom_pairings: TwoStrTupleList=None,
                      custom_low_minus_high_dict: StrBoolDict=None) -> pd.DataFrame:

    _validate_model(
        factor_model,
        custom_labels=custom_labels,
        custom_groupvar_ngroups_dict=custom_groupvar_ngroups_dict,
        custom_pairings=custom_pairings,
        custom_low_minus_high_dict=custom_low_minus_high_dict
    )

    custom_labels, custom_groupvar_ngroups_dict, custom_pairings, custom_low_minus_high_dict = _standardize_custom_args(
        custom_labels=custom_labels,
        custom_groupvar_ngroups_dict=custom_groupvar_ngroups_dict,
        custom_pairings=custom_pairings,
        custom_low_minus_high_dict=custom_low_minus_high_dict
    )

    byvars = _to_list_if_str(byvars)

    default_varnames = dict(
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var
    )
    # Convert to portfolio names
    default_portfolio_varnames = {
        key: _other_groupvar_portname(value) if value else value for key, value in default_varnames.items()
    }

    ff_portfolios = create_ff_portfolios(
        df,
        factor_model=factor_model,
        byvars=byvars,
        id_var=id_var,
        date_var=datevar,
        custom_groupvar_ngroups_dict=custom_groupvar_ngroups_dict,
        **default_varnames
    )

    labels = get_and_set_labels(
        ff_portfolios,
        factor_model=factor_model,
        custom_labels=custom_labels,
        **default_portfolio_varnames
    )

    pairings = create_dual_sort_variables_get_pairings(
        ff_portfolios,
        factor_model=factor_model,
        custom_pairings=custom_pairings,
        **default_portfolio_varnames
    )

    if byvars is not None:
        base_vars = byvars + [datevar]
    else:
        base_vars = [datevar]

    base_df = ff_portfolios.loc[:,base_vars].drop_duplicates()
    for pairing in pairings:
        minus_vars_df = construct_averges_and_minus_variables_for_pairing(
            df=ff_portfolios,
            pairing=pairing,
            labels=labels,
            factor_model=factor_model,
            datevar=datevar,
            byvars=byvars,
            retvar=retvar,
            wtvar=wtvar,
            custom_low_minus_high_dict=custom_low_minus_high_dict,
            **default_portfolio_varnames
        )
        base_df = base_df.merge(minus_vars_df, how='left', on=base_vars)

    return base_df


def construct_averges_and_minus_variables_for_pairing(df: pd.DataFrame, pairing: TwoStrTuple,
                                                      labels: DictofStrsandStrLists, factor_model: StrOrInt,
                                                      datevar='Date', byvars: ListOrStr=None,
                                                      retvar: str='RET', wtvar: str='Market Equity',
                                                      size_var: str = None, value_var: str = None,
                                                      profitability_var: str = None, investment_var: str = None,
                                                      custom_low_minus_high_dict: StrBoolDict=None) -> pd.DataFrame:

    dual_portname = _dual_sort_varname(pairing[0], pairing[1])

    averages_long_shape = portfolio_returns(
        df,
        retvar=retvar,
        datevar=datevar,
        wtvar=wtvar,
        byvars=byvars,
        portvar=dual_portname
    )

    wide = long_averages_to_wide_averages(
        averages_long_shape,
        datevar=datevar,
        retvar=retvar,
        byvars=byvars,
        dual_portvar=dual_portname
    )

    return construct_minus_variables(
        wide,
        labels=labels,
        pairing=pairing,
        factor_model=factor_model,
        byvars=byvars,
        datevar=datevar,
        size_var=size_var,
        value_var=value_var,
        profitability_var=profitability_var,
        investment_var=investment_var,
        custom_low_minus_high_dict=custom_low_minus_high_dict
    )