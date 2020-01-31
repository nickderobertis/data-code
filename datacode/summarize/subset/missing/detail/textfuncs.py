from datacode.typing import IntOrNone, FloatOrNone, Union

def missing_more_than_str(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone,
                          missing_display_str: str, id_var: str) -> str:
    return f'More than {_t_or_q(missing_tolerance, missing_quantile)} {missing_display_str} {id_var}'

def missing_more_than_pct_str(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone,
                              missing_display_str: str, id_var: str) -> str:
    return f'More than {_t_or_q(missing_tolerance, missing_quantile)} {missing_display_str} {id_var} Percentage'

def missing_tolerance_count_str(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone,
                                missing_display_str: str, id_var: str) -> str:
    return f'{_t_or_q(missing_tolerance, missing_quantile)} or less {missing_display_str} {id_var} Count'

def id_count_str(id_var: str) -> str:
    return f'{id_var} Count'

def num_or_pct(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone) -> str:
    if missing_quantile is not None:
        return '%'

    return '#'

def num_or_pct_word(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone) -> str:
    if missing_quantile is not None:
        return 'percentage'

    return 'number'

def pct_of_if_necessary(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone) -> str:
    if missing_quantile is not None:
        return '% of'

    return ''

def _t_or_q(missing_tolerance: IntOrNone, missing_quantile: FloatOrNone) -> Union[int, str]:
    if missing_quantile is not None:
        return f'{missing_quantile * 100:g}%'

    return missing_tolerance