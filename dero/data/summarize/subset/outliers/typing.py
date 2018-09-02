import pandas as pd
from typing import List, Dict, Tuple, Union, Sequence

StrList = List[str]
FloatSequence = Sequence[float]
AssociatedColDict = Dict[str, StrList]
BoolDict = Dict[str, bool]
DfDict = Dict[str, pd.DataFrame]
TwoDfDictTuple = Tuple[DfDict, DfDict]
TwoDfDictAndDfTuple = Tuple[DfDict, DfDict, pd.DataFrame]
IntOrFloat = Union[int, float]
MinMaxTuple = Tuple[IntOrFloat, IntOrFloat]
MinMaxDict = Dict[str, MinMaxTuple]
