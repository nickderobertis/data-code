

def lag(orig: str, num_lags: int = 1) -> str:
    return orig + f'_{{t - {num_lags}}}'
