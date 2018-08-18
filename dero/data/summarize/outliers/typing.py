import pandas as pd
from dero.latex import Document
from dero.latex.table import Table
from typing import List, Dict, Tuple, Union

StrList = List[str]
AssociatedColDict = Dict[str, StrList]
BoolDict = Dict[str, bool]
DfDict = Dict[str, pd.DataFrame]
TwoDfDictTuple = Tuple[DfDict, DfDict]
IntOrFloat = Union[int, float]
MinMaxTuple = Tuple[IntOrFloat, IntOrFloat]
MinMaxDict = Dict[str, MinMaxTuple]
Tables = List[Table]
DocumentOrTables = Union[Document, Tables]