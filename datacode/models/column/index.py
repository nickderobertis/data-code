from typing import Sequence

from datacode.models.index import Index
from datacode.models.variables import Variable


class ColumnIndex:

    def __init__(self, index: Index, variables: Sequence[Variable]):
        self.index = index
        self.variables = variables

    def __eq__(self, other):
        if not isinstance(other, ColumnIndex):
            return False

        return all([
            self.index == other.index,
            sorted(self.variables) == sorted(other.variables)
        ])
