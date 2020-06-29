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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datacode.models.pipeline.base import DataPipeline


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
