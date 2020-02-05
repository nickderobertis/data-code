"""
A framework to manage variables. Annotates data such that transformations of a variable are seen as that and not
as new variables. Manages naming the variable across transformations.
"""
from .collection import (
    VariableCollection,
    VariableDisplayNameCollection,
    VariableNameCollection,
    Variable,
    VariableChangeNameCollection,
    VariablePortfolioNameCollection,
    VariableChangePortfolioNameCollection
)