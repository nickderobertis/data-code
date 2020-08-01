import inspect
import re
from typing import Any, Iterable, Dict, Sequence, Type, List, Union, Optional

from deepdiff import DeepHash
from typing_extensions import TypedDict


class HashDictOptions(TypedDict, total=False):
    exclude_types: List[Type]
    exclude_paths: Union[str, List[str]]
    exclude_regex_paths: Union[str, List[str]]
    ignore_type_subclasses: bool


class DeterministicHashDictMixin:
    hash_dict_options: HashDictOptions = dict(
        exclude_regex_paths=[
            ".df$",
            "._df$",
            ".series$",
            ".data_loader$",
            ".forward_links$",
            ".back_links$",
            "._node_id$",
            "._last_modified$",
            "._operations$",
            "._operation_index$",
            ".repr_cols$",
            ".result$",
            ".transform.key$",
        ]
    )

    def hash_dict(self) -> Dict[str, str]:
        dh = DeepHash(self, **self.hash_dict_options)
        out_dict: Dict[str, str] = {}
        exclude_regex = self._exclude_regex
        for key in self.__dict__:
            if exclude_regex:
                if exclude_regex.fullmatch(key):
                    continue
            value = getattr(self, key)
            if inspect.ismethod(value):
                continue
            out_dict[key] = dh[value]

        return out_dict

    @property
    def _exclude_regex(self) -> Optional[re.Pattern]:
        if 'exclude_regex_paths' not in self.hash_dict_options:
            return None

        rp = self.hash_dict_options['exclude_regex_paths']
        if isinstance(rp, str):
            all_rp = [rp]
        else:
            all_rp = rp

        rp_str = '|'.join([r.lstrip('.') for r in all_rp])
        return re.compile(rp_str)
