from typing import Sequence

from datacode.models.transform.transform import Transform


def register_transforms(transforms: Sequence[Transform]):
    """
    Convenience function to add passed transforms into DEFAULT_TRANSFORMS which are automatically
    provided to all variables

    :param transforms:
    :return:
    """
    from datacode.models.transform.specific import DEFAULT_TRANSFORMS

    for transform in transforms:
        existing_keys = [transform.key for transform in DEFAULT_TRANSFORMS]
        if transform.key not in existing_keys:
            DEFAULT_TRANSFORMS.append(transform)
