from typing import Dict, Union, Tuple, Optional, List
import pandas as pd

from dero.latex import Document
from dero.latex.table import Table
from dero.latex.figure import Figure

SimpleDfDict = Dict[str, pd.DataFrame]
DfOrDfDict = Union[SimpleDfDict, pd.DataFrame]
DfDict = Dict[str, DfOrDfDict]
DfDictOrNone = Union[DfDict, None]
DfOrSeries = Union[pd.DataFrame, pd.Series]
DfTuple = Tuple[DfOrSeries, DfOrSeries]
DfTupleOrNone = Optional[DfTuple]
FloatList = List[float]
StrList = List[str]
StrOrNone = Union[str, None]
DocumentOrTable = Union[Document, Table]
Tables = List[Table]
DocumentOrTables = Union[Document, Tables]
DocumentOrTablesOrNone = Union[DocumentOrTables, None]
LatexObj = Union[Table, Figure]
LatexObjs = List[LatexObj]
DocumentOrLatexObjs = Union[Document, LatexObjs]