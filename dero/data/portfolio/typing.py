from typing import Union, Tuple, Iterable, List

PortfolioName = Union[str, int]
PortfolioPair = Tuple[PortfolioName, PortfolioName]
PortfolioPairs = Iterable[PortfolioPair]

StrList = List[str]
StrListList = List[StrList]