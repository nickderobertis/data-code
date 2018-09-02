from typing import Dict, Union, Tuple, Optional, List
import pandas as pd


SimpleDfDict = Dict[str, pd.DataFrame]
DfOrDfDict = Union[SimpleDfDict, pd.DataFrame]
DfDict = Dict[str, DfOrDfDict]
DfDictOrNone = Union[DfDict, None]
DfOrSeries = Union[pd.DataFrame, pd.Series]
DfTuple = Tuple[DfOrSeries, DfOrSeries]
DfTupleOrNone = Optional[DfTuple]
FloatList = List[float]
StrList = List[str]