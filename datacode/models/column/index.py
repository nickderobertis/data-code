from typing import Sequence

from datacode.models.index import Index
from datacode.models.variables import Variable


class ColumnIndex:

    def __init__(self, index: Index, variables: Sequence[Variable]):
        self.index = index
        self.variables = variables
