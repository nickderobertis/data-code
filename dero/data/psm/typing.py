from typing import List, Optional, Tuple, Any, Dict, Union
import pyexlatex.table as lt
import pandas as pd

StrList = List[str]
StrListOrNone = Optional[StrList]
RegResultSummaryTuple = Tuple[Any, pd.DataFrame]
StrOrNone = Optional[str]
TwoDfTuple = Tuple[pd.DataFrame, pd.DataFrame]
DfDict = Dict[str, pd.DataFrame]
FloatOrNone = Optional[float]
Summary = Union[DfDict, lt.Table]
DfSummaryTuple = Tuple[pd.DataFrame, Summary]