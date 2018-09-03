

def missing_more_than_str(missing_tolerance: int, missing_display_str: str, id_var: str) -> str:
    return f'More than {missing_tolerance} {missing_display_str} {id_var}'

def missing_more_than_pct_str(missing_tolerance: int, missing_display_str: str, id_var: str) -> str:
    return f'More than {missing_tolerance} {missing_display_str} {id_var} Percentage'

def id_count_str(id_var: str) -> str:
    return f'{id_var} Count'