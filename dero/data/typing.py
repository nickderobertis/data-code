from typing import Dict, Union, Tuple, Optional, List, Sequence
import pandas as pd

from pyexlatex import Document
from pyexlatex.table import Table
from pyexlatex.figure import Figure

SimpleDfDict = Dict[str, pd.DataFrame]
DfOrDfDict = Union[SimpleDfDict, pd.DataFrame]
DfDict = Dict[str, DfOrDfDict]
DfDictOrNone = Union[DfDict, None]
DfOrSeries = Union[pd.DataFrame, pd.Series]
DfTuple = Tuple[DfOrSeries, DfOrSeries]
DfTupleOrNone = Optional[DfTuple]
FloatList = List[float]
FloatOrNone = Union[float, None]
FloatSequence = Sequence[float]
FloatSequenceOrNone = Union[FloatSequence, None]
StrList = List[str]
StrOrNone = Union[str, None]
DocumentOrTable = Union[Document, Table]
Tables = List[Table]
DocumentOrTables = Union[Document, Tables]
DocumentOrTablesOrNone = Union[DocumentOrTables, None]
LatexObj = Union[Table, Figure]
LatexObjs = List[LatexObj]
DocumentOrLatexObjs = Union[Document, LatexObjs]
IntSequence = Sequence[int]
IntSequenceOrNone = Union[IntSequence, None]
IntOrNone = Union[int, None]