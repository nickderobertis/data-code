import pandas as pd
from typing import Union, List, Tuple, Dict

StrList = List[str]
ListOrStr = Union[List, str]
DfListListStrTuple = Tuple[pd.DataFrame, List, List, str]
GroupvarNgroupsDict = Dict[str, int]
StrOrInt = Union[int, str]
DictofStrsandStrLists = Dict[str, StrList]
TwoStrTuple = Tuple[str, str]
TwoStrTupleList = List[TwoStrTuple]
StrBoolDict = Dict[str, bool]
TwoStrListTuple = Tuple[StrList, StrList]