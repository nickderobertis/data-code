from typing import Dict, TYPE_CHECKING, Optional, Callable, Any

if TYPE_CHECKING:
    from datacode.models.variables.collection import VariableCollection, Variable

PrefixVariableCollectionDict = Dict[str, 'VariableCollection']
StrFunc = Optional[Callable[[str], str]]
ValueFunc = Optional[Callable[[Any], Any]]