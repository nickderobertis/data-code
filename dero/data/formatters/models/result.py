from typing import Union, Tuple
from dero.data.formatters.stars import parse_stars_value

class Result:

    def __init__(self, value: str):
        self.str_result, self.result, self.num_stars = parse_value(value)

    def __repr__(self):
        return f'<Result({self})>'

    def __str__(self):
        return f'{self.str_result}{"*" * self.num_stars}'

    @property
    def is_significant(self) -> bool:
        return self.num_stars > 0

    def is_significant_at(self, pct: int) -> bool:
        """

        Args:
            pct: 90 - 99 of significance level, e.g. 90, 95, or 99

        Returns: bool

        """
        if pct < 90:
            raise ValueError('cannot determine significance below 90%')
        elif pct < 95:
            return self.num_stars > 0
        elif pct < 99:
            return self. num_stars > 1
        elif pct == 99:
            return self. num_stars > 2
        else:
            raise ValueError('cannot determine significance above 99%')

    @property
    def is_positive(self) -> bool:
        try:
            return self.result > 0
        except TypeError:
            return False

    @property
    def is_negative(self) -> bool:
        try:
            return self.result < 0
        except TypeError:
            return False



def parse_value(value: str) -> Tuple[str, Union[str, float], int]:
    result, stars = parse_stars_value(value)
    num_result = to_numeric_if_numeric(result)
    num_stars = len(stars)
    return result, num_result, num_stars



def to_numeric_if_numeric(value: str) -> Union[str, float]:
    try:
        return float(value)
    except ValueError:
        return value