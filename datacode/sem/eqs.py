from typing import Dict, Sequence, List

import pyexlatex as pl

from datacode.models.variables import Variable


def model_eqs(structural_dict: Dict[Variable, Sequence[Variable]],
              measurement_dict: Dict[Variable, Sequence[Variable]],
              var_corr_groups: Sequence[Sequence[Variable]]) -> List[pl.Equation]:
    all_eqs = []
    for y, x_vars in structural_dict.items():
        all_vars = [y, *x_vars]
        all_eqs.append(_vars_to_eq(all_vars, operator='='))
    for y, x_vars in measurement_dict.items():
        all_vars = [y, *x_vars]
        all_eqs.append(_vars_to_eq(all_vars, operator='='))
    for corr_group in var_corr_groups:
        all_eqs.append(_vars_to_eq(corr_group, operator='~~'))
    valid_eqs = [eq for eq in all_eqs if eq]
    return valid_eqs


def _vars_to_eq(var_seq: Sequence[Variable], operator: str = '=', inline: bool = False):
    if len(var_seq) < 2 or (len(var_seq) == 2 and var_seq[0] == var_seq[1]):
        return None

    lhs = var_seq[0].symbol
    rhs = ' + '.join([str(var.symbol) for var in var_seq[1:]])

    if operator == '=':
        eq_str = f'{lhs} = {rhs}'
    elif operator == '~~':
        eq_str = rf'{lhs} \sim {rhs}'
    else:
        raise NotImplementedError(f'operator {operator} not supported')
    return pl.Equation(str_eq=eq_str, inline=inline)