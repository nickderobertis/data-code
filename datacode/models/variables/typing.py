from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from datacode.models.variables.collection import VariableCollection, Variable

PrefixVariableCollectionDict = Dict[str, 'VariableCollection']