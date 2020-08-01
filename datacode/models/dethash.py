import inspect
import re
from typing import Any, Iterable, Dict, Sequence, Type, List, Union, Optional, Callable, Tuple
import logging

from deepdiff import DeepHash
import numpy as np
from typing_extensions import TypedDict


# Suppress warnings about attributes not able to be hashed
dh_logger = logging.getLogger('deepdiff.deephash')
dh_logger.setLevel(logging.ERROR)


class HashDictOptions(TypedDict, total=False):
    exclude_types: List[Type]
    exclude_paths: Union[str, List[str]]
    exclude_regex_paths: Union[str, List[str]]
    exclude_obj_callback: Callable[[Any, str], bool]
    ignore_type_subclasses: bool
    hasher: Callable[[str], str]
    ignore_repetition: bool
    significant_digits: int
    number_format_notation: str
    ignore_type_in_groups: Union[bool, Sequence[Union[Type, Tuple[Type, Type]]]]
    ignore_string_type_changes: bool
    ignore_numeric_type_changes: bool
    ignore_string_case: bool

DEFAULT_HASH_DICT_OPTIONS: HashDictOptions = dict(
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
            "._pre_execute_hash_dict$"
        ],
        exclude_types=[np.dtype],
        ignore_type_subclasses=True,
    )


class DeterministicHashDictMixin:
    hash_dict_options: HashDictOptions = DEFAULT_HASH_DICT_OPTIONS

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
            try:
                out_dict[key] = dh[value]
            except KeyError:
                # hash was not calculated for this item for some other reason
                continue

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
