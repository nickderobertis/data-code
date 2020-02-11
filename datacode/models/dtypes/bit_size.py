import re
from typing import Optional

DTYPE_PATTERN = re.compile(r'[a-z_]+(\d+)?')

def get_bit_from_dtype(dtype: str) -> Optional[int]:
    match = DTYPE_PATTERN.match(dtype)
    if not match:
        raise ValueError(f'could not parse {dtype} as dtype')
    str_or_none = match.group(1)
    if str_or_none:
        # Got an int as a str
        return int(str_or_none)

    # Got None, just return it
    if str_or_none is None:
        return str_or_none

    raise ValueError