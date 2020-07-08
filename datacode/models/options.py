from typing import Any, Tuple, Dict


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

    _orig_class_attrs: Dict[Tuple[str, str], Any] = {}

    def reset(self):
        """
        Undo any changes made through the options interface
        :return:
        """
        for (class_name, attr), orig_value in self._orig_class_attrs.items():
            self._set_class_attr(class_name, attr, orig_value)
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

    def _set_class_attr(self, class_name: str, attr: str, value: Any) -> Any:
        import datacode as dc

        klass = getattr(dc, class_name)
        orig_value = getattr(klass, attr)
        setattr(klass, attr, value)
        return orig_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset()


options = DatacodeOptions()
