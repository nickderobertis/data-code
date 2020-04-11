from typing import Sequence, Dict

from datacode.models.variables.variable import Variable


def model_str(structural_dict: Dict[Variable, Sequence[Variable]],
              measurement_dict: Dict[Variable, Sequence[Variable]],
              var_corr_groups: Sequence[Sequence[Variable]]) -> str:
    m_str = '# structural part\n'
    for y, x_vars in structural_dict.items():
        all_vars = [y, *x_vars]
        m_str += _vars_to_structural_str(all_vars)
        m_str += '\n'
    m_str += '\n# measurement part\n'
    for y, x_vars in measurement_dict.items():
        all_vars = [y, *x_vars]
        m_str += _vars_to_measurement_str(all_vars)
        m_str += '\n'
    m_str += '\n# correlations\n'
    for corr_group in var_corr_groups:
        m_str += _vars_to_correlated_str(corr_group)
        m_str += '\n'
    return m_str


def _vars_to_add_keys_str(var_seq: Sequence[Variable]) -> str:
    return ' + '.join([var.unique_key for var in var_seq])


def _vars_to_structural_str(var_seq: Sequence[Variable]) -> str:
    return _vars_to_eq_str(var_seq, '~')


def _vars_to_measurement_str(var_seq: Sequence[Variable]) -> str:
    return _vars_to_eq_str(var_seq, '=~')


def _vars_to_correlated_str(var_seq: Sequence[Variable]) -> str:
    return _vars_to_eq_str(var_seq, '~~')


def _vars_to_eq_str(var_seq: Sequence[Variable], eq_sym: str = '~') -> str:
    if len(var_seq) < 2:
        return ''

    return f'{var_seq[0].unique_key} {eq_sym} ' + _vars_to_add_keys_str(var_seq[1:])

