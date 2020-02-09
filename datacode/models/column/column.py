from typing import Optional

from datacode.models.column.index import ColumnIndex
from datacode.models.variables import Variable


class Column:

    def __init__(self, variable: Variable, index: Optional[ColumnIndex] = None):
        self.variable = variable
        self.index = index
