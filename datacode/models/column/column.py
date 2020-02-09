from typing import Optional, Sequence

from datacode.models.column.index import ColumnIndex
from datacode.models.variables import Variable


class Column:

    def __init__(self, variable: Variable, indices: Optional[Sequence[ColumnIndex]] = None):
        self.variable = variable
        self.indices = indices
