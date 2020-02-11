from typing import Optional, Sequence

from datacode.models.column.index import ColumnIndex
from datacode.models.variables import Variable


class Column:

    def __init__(self, variable: Variable, indices: Optional[Sequence[ColumnIndex]] = None,
                 applied_transform_keys: Optional[Sequence[str]] = None):
        if applied_transform_keys is None:
            applied_transform_keys = []

        self.variable = variable
        self.indices = indices
        self.applied_transform_keys = applied_transform_keys
