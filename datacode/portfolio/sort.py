from functools import partial
import pandas as pd

from datacode.portfolio.name import(
    _is_differenced_port_name,
    _ports_from_differenced_port_name
)

def sort_portfolio_index(df, inplace=False):
    if not inplace:
        df = df.copy()

    num_ports = len(df.index.unique())
    _port_sort_key = partial(_port_sort_value, num_ports=num_ports)
    index_name = df.index.name
    index_access_name = 'index' if index_name is None else index_name
    df.reset_index(inplace=True)
    df['_sort_value'] = df[index_access_name].apply(_port_sort_key).astype(int)
    df.sort_values('_sort_value', inplace=True)
    df.set_index(index_access_name, inplace=True)
    df.drop('_sort_value', axis=1, inplace=True)
    df.index.name = index_name

    if not inplace:
        return df

def sort_portfolio_columns(df):
    """
    Note: creates a copy, not inplace
    """
    return sort_portfolio_index(df.T).T

def sort_portfolio_columns_and_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Note: creates a copy, not inplace
    """
    df = sort_portfolio_index(df, inplace=False)
    return sort_portfolio_columns(df)

def _port_sort_value(int_or_str, num_ports=3):
    if _is_int(int_or_str):
        return int(int_or_str)

    if not isinstance(int_or_str, str):
        raise ValueError(f'must pass str or int, got {int_or_str} of type {type(int_or_str)}')

    name_dict = {
        'high': num_ports,
        'mid': 2, # mid only appears when there are three ports
        'low': 1,
        'zero': 0
    }
    name = int_or_str.lower().strip()

    if name in name_dict:
        return name_dict[name]

    # If this is a differenced portfolio, use the sum of the sort values of the individual, plus the max port.
    # This way differenced portfolios will always come after other portfolios, but then will be ordered consistently
    if _is_differenced_port_name(name):
        long_port, short_port = _ports_from_differenced_port_name(name)
        long_port_sort_value = _port_sort_value(long_port, num_ports=num_ports)
        short_port_sort_value = _port_sort_value(short_port, num_ports=num_ports)
        return long_port_sort_value + short_port_sort_value + num_ports # num ports to ensure after individual ports


    raise ValueError(f'passed str was not an int or special port name high, low, or zero. got {int_or_str}')


def _is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False