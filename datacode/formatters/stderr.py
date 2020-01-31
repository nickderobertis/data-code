from numpy import nan
import pandas as pd
import re

RESULT_PATTERN = re.compile(r'\(([\d.,]+)\)')


def convert_to_stderr_format(value: float, num_decimals: int = 2) -> str:
    if pd.isnull(value):
        return ''
    return f'({value:.{num_decimals}f})'


def parse_stderr_value(value: str) -> float:
    match = RESULT_PATTERN.fullmatch(value)
    if not match:
        return nan

    result = match.group(1)
    return float(result)
