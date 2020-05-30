from typing import Sequence, Dict, Optional, Type

from datacode.models.dtypes.base import DataType


class DataTypeManager:

    def __init__(self, data_types: Sequence[Type[DataType]]):
        self.data_types = data_types
        self.name_map = self._create_name_map()
        self.root_name_map = self._create_name_map('name_roots', existing_maps=[self.name_map])

    def _create_name_map(self, name_attr: str = 'names',
                         existing_maps: Optional[Sequence[Dict[str, Type[DataType]]]] = None
                         ) -> Dict[str, Type[DataType]]:
        all_maps = {}
        if existing_maps is not None:
            for e_map in existing_maps:
                all_maps.update(e_map)

        name_map = {}
        for data_type in self.data_types:
            for name in getattr(data_type, name_attr):
                if name in all_maps:
                    raise ValueError(f'Got name {name} for data type {data_type} when it was already '
                                     f'used by data type {name_map[name]}')
                name_map[name] = data_type
                all_maps[name] = data_type
        return name_map

    def get_by_name(self, name: str) -> DataType:
        name = name.lower()
        # Try for exact match first
        try:
            cls = self.name_map[name]
            return cls.from_str(name)
        except KeyError:
            pass

        # Now look up in root name dict
        for dt_name, dt in self.root_name_map.items():
            if name.startswith(dt_name):
                cls = dt
                return cls.from_str(name)

        # Didn't find in either dict
        raise KeyError(name)
