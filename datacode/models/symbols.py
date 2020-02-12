from typing import Optional, Union

from sympy import Symbol


def to_symbol_if_necessary(sym: Optional[Union[str, Symbol]]) -> Optional[Symbol]:
    if isinstance(sym, Symbol) or sym is None:
        return sym

    return str_to_symbol(sym)


def str_to_symbol(sym: str) -> Symbol:
    return Symbol(sym)


def var_key_to_symbol_str(var_key: str) -> str:
    parts = var_key.split('_')
    if len(parts) == 1:
        return rf'\text{{{parts[0].title()}}}'
    elif len(parts) > 1:
        return "".join([part[0].upper() for part in parts])
    else:
        raise ValueError(f'got invalid var_key {var_key}')
