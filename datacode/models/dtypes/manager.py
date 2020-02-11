from typing import Sequence, Dict

from datacode.models.dtypes.base import DataType


class DataTypeManager:

    def __init__(self, data_types: Sequence[DataType]):
        self.data_types = data_types
        self.name_map = self._create_name_map()

    def _create_name_map(self) -> Dict[str, DataType]:
        name_map = {}
        for data_type in self.data_types:
            for name in data_type.names:
                if name in name_map:
                    raise ValueError(f'Got name {name} for data type {data_type} when it was already '
                                     f'used by data type {name_map[name]}')
                name_map[name] = data_type
        return name_map

    def get_by_name(self, name: str):
        return self.name_map[name]
