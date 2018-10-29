

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