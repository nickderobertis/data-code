from typing import Sequence

from mixins import ReprMixin

from datacode.models.index import Index
from datacode.models.variables import Variable


class ColumnIndex(ReprMixin):
    repr_cols = ['index', 'variables']

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
