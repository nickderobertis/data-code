
from datacode.models.variables.typing import StrFunc
from datacode.models.logic.partial import partial


class Transform:

    def __init__(self, key: str, name_func: StrFunc = None, data_func: StrFunc = None):
        self.key = key
        self.name_func = name_func
        self.data_func = data_func


class AppliedTransform(Transform):

    def __init__(self, key: str, *args, name_func: StrFunc = None, data_func: StrFunc = None, **kwargs):
        super().__init__(key, name_func=name_func, data_func=data_func)
        self.args = args
        self.kwargs = kwargs

        if self.name_func is not None:
            self.name_func = partial(self.name_func, ..., *args, **kwargs)
        if self.data_func is not None:
            self.data_func = partial(self.data_func, ..., *args, **kwargs)

    @classmethod
    def from_transform(cls, transform: Transform, *args, **kwargs):
        obj = cls(transform.key, *args, name_func=transform.name_func, data_func=transform.data_func, **kwargs)
        return obj
