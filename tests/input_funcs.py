

def lag(orig: str, num_lags: int) -> str:
    return orig + f'_{{t - {num_lags}}}'
