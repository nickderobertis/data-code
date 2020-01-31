import re
from typing import Tuple

from datacode.portfolio.typing import PortfolioPair, PortfolioName


def _differenced_port_name(difference_port: PortfolioPair =('High', 'Low')) -> str:
    return f'{difference_port[0]} - {difference_port[1]}'


def _is_differenced_port_name(portname: PortfolioName) -> bool:
    pattern = re.compile(r'[\d\w]+ \- [\d\w]+')
    if pattern.fullmatch(str(portname)):
        return True
    else:
        return False


def _ports_from_differenced_port_name(portname: PortfolioName) -> Tuple[str, str]:
    long_port, short_port = str(portname).split(' - ')
    return long_port, short_port