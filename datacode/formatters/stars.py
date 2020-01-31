from typing import Tuple
import re

RESULT_PATTERN = re.compile(r'([^\*]+)(\**)')

def convert_to_stars(t_value: float) -> str:
    t = abs(t_value)
    if t < 1.645:
        return ''
    elif t < 1.96:
        return '*'
    elif t < 2.576:
        return '**'
    elif t > 2.576:
        return '***'
    else: # covers nan
        return ''


def parse_stars_value(value: str) -> Tuple[str, str]:
    match = RESULT_PATTERN.fullmatch(value)
    if not match:
        return '', ''

    result = match.group(1)
    stars = match.group(2)
    return result, stars