"""
Hooks to run functions during datacode operations globally

:Usage:

    >>> import datacode.hooks as dc_hooks
    ...
    >>> def log_begin_execute(pipeline: DataPipeline):
    ...     print(f"running {pipeline.name}")
    ...
    >>> dc_hooks.on_begin_execute_pipeline = log_begin_execute

:All the Hooks:

    Each hook may expect different arguments and have different return values.
    Here is the list of the hooks, along with the expected arguments and return values.

"""
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd


if TYPE_CHECKING:
    from datacode.models.pipeline.base import DataPipeline
    from datacode.models.pipeline.operations.operation import DataOperation
    from datacode.models.source import DataSource

_orig_locals = {key: value for key, value in locals().items() if not key.startswith('_')}


def on_begin_execute_pipeline(pipeline: 'DataPipeline') -> None:
    """
    Called at the beginning of :meth:`DataPipeline.execute`

    :param pipeline: The pipeline which is about to be executed
    :return: None
    """
    pass


def on_end_execute_pipeline(pipeline: 'DataPipeline') -> None:
    """
    Called at the end of :meth:`DataPipeline.execute`

    :param pipeline: The pipeline which was just executed
    :return: None
    """
    pass


def on_begin_execute_operation(operation: 'DataOperation') -> None:
    """
    Called at the beginning of :meth:`DataOperation.execute`

    :param operation: The operation which is about to be executed
    :return: None
    """
    pass


def on_end_execute_operation(operation: 'DataOperation') -> None:
    """
    Called at the end of :meth:`DataOperation.execute`

    :param operation: The operation which was just executed
    :return: None
    """
    pass


def on_begin_load_source(source: 'DataSource') -> None:
    """
    Called at the beginning of :meth:`DataSource._load`, which
    is usually called when loading :paramref:`.DataSource.df`
    from either location or pipeline.

    :param source: DataSource for which DataFrame is about to be loaded
    :return:
    """
    pass


def on_end_load_source(source: 'DataSource', df: pd.DataFrame) -> pd.DataFrame:
    """
    Called at the end of :meth:`DataSource._load`, which
    is usually called when loading :paramref:`.DataSource.df`
    from either location or pipeline.

    The DataFrame returned from this function will be
    set as :paramref:`.DataSource.df`

    :param source: DataSource for which DataFrame was just loaded
    :param df:
    :return: DataFrame which will be set as :paramref:`.DataSource.df`
    """
    return df


_new_locals = {key: value for key, value in locals().items() if not key.startswith('_')}
_hook_keys = [key for key in _new_locals if key not in _orig_locals]
_orig_hooks = deepcopy({key: value for key, value in _new_locals.items() if key in _hook_keys})


def reset_hooks() -> None:
    """
    Go back to original dummy hooks, removes all user settings of hooks

    :return: None

    :Notes:

        This is the only function in the module which is not a hook itself.
        Instead it is a utility method meant to be called by the user.

    """
    for key, value in _orig_hooks.items():
        globals()[key] = value


__all__ = _hook_keys + ['reset_hooks']
