from typing import Any, Tuple, Dict


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

    _orig_class_attrs: Dict[Tuple[str, str], PreservedAttribute] = {}

    def reset(self):
        """
        Undo any changes made through the options interface
        :return:
        """
        for (class_name, attr), orig_value in self._orig_class_attrs.items():
            if orig_value.attr_existed:
                self._set_class_attr(
                    class_name,
                    attr,
                    orig_value.value,
                )
            else:
                self._delete_class_attr(class_name, attr)
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
        orig_value = self._set_class_attr(class_name, attr, value)
        self._orig_class_attrs[(class_name, attr)] = orig_value
        return self

    def _set_class_attr(
        self, class_name: str, attr: str, value: Any,
    ) -> PreservedAttribute:
        import datacode as dc

        klass = getattr(dc, class_name)

        try:
            orig_value = getattr(klass, attr)
            has_attr = True
        except AttributeError:
            orig_value = None
            has_attr = False

        setattr(klass, attr, value)
        return PreservedAttribute(attr, orig_value, attr_existed=has_attr)

    def _delete_class_attr(self, class_name: str, attr: str):
        import datacode as dc

        klass = getattr(dc, class_name)
        delattr(klass, attr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset()


options = DatacodeOptions()
