from typing import Any, Tuple, Dict, Type

from datacode.logger import logger
from datacode.models.dethash import HashDictOptions, DeterministicHashDictMixin


class PreservedAttribute:
    def __init__(self, attr: str, value: Any, attr_existed: bool = True):
        self.attr = attr
        self.value = value
        self.attr_existed = attr_existed


class DatacodeOptions:
    """
    Allows setting options for the datacode library

    :Usage:

    Use as a context manager with a single change:

    >>> with dc.options.set_class_attr("DataSource", "copy_keys", ['a', 'b']):
    >>>     # Do something
    >>> # Now options have been reset

    Usage as a context manager with multiple changes:

    >>> with dc.options:
    >>>     dc.options.set_class_attr("DataSource", "copy_keys", ['a', 'b'])
    >>>     # More options changes, then desired operations

    Plain usage:

    >>> dc.options.set_class_attr("DataSource", "copy_keys", ['a', 'b'])
    >>> # Now change lasts, until (optionally) calling
    >>> dc.options.reset()
    """

    _orig_class_attrs: Dict[Tuple[Type, str], PreservedAttribute] = {}

    def reset(self):
        """
        Undo any changes made through the options interface
        :return:
        """
        logger.debug(f"Resetting datacode options")
        for (klass, attr), orig_value in self._orig_class_attrs.items():
            if orig_value.attr_existed:
                setattr(
                    klass, attr, orig_value.value,
                )
            else:
                delattr(klass, attr)
        self._orig_class_attrs = {}

    def set_class_attr(
        self, class_name: str, attr: str, value: Any
    ) -> "DatacodeOptions":
        """
        Sets an attribute on a datacode class

        :param class_name: Name of a class in the main datacode namespace
        :param attr: Attribute to be updated on the class
        :param value: Value to set the attribute to
        :return: same options instance
        """
        import datacode as dc

        logger.debug(
            f"Setting datacode options for class attr {class_name}.{attr} to {value}"
        )

        klass = getattr(dc, class_name)
        self._set_class_attr(klass, attr, value)
        return self

    def set_hash_options(self, options: HashDictOptions) -> 'DatacodeOptions':
        """
        Control how pipelines determine whether the result is cached. Each time
        a pipeline is run it calculates a hash of its options
        and its data sources to determine whether it has already run with the
        same settings.

        ``deepdiff.DeepHash` is used to determine the hash. DeepHash options
        can be passed to control the hashing behavior. The :py:class:`HashDictOptions`
        typed dictionary is provided for convenience and matches those options

        :param options: dict of kwargs that would be passed to ``deepdiff.DeepHash``
        :return: same options instance
        """
        self._set_class_attr(DeterministicHashDictMixin, 'hash_dict_options', options)
        return self

    def _set_class_attr(
        self, klass: Type, attr: str, value: Any,
    ):
        orig_value = _set_class_attr(klass, attr, value)
        self._orig_class_attrs[(klass, attr)] = orig_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset()


def _set_class_attr(klass: Type, attr: str, value: Any):
    try:
        orig_value = getattr(klass, attr)
        has_attr = True
    except AttributeError:
        orig_value = None
        has_attr = False

    setattr(klass, attr, value)
    return PreservedAttribute(attr, orig_value, attr_existed=has_attr)


options = DatacodeOptions()
