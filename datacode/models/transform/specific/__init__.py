from datacode.models.transform.specific.change import change_transform
from datacode.models.transform.specific.lag import lag_transform

DEFAULT_TRANSFORMS = [
    lag_transform,
    change_transform,
]
