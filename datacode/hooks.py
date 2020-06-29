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
from typing import TYPE_CHECKING, Tuple

import pandas as pd


if TYPE_CHECKING:
    from datacode.models.pipeline.base import DataPipeline
    from datacode.models.pipeline.operations.operation import DataOperation
    from datacode.models.source import DataSource
    from datacode.models.column.column import Column
    from datacode.models.variables.variable import Variable
    from datacode.models.transform.transform import Transform
    from datacode.models.transform.source import SourceTransform

_orig_locals = {
    key: value for key, value in locals().items() if not key.startswith("_")
}


def on_begin_execute_pipeline(pipeline: "DataPipeline") -> None:
    """
    Called at the beginning of :meth:`DataPipeline.execute`

    :param pipeline: The pipeline which is about to be executed
    :return: None
    """
    pass


def on_end_execute_pipeline(pipeline: "DataPipeline") -> None:
    """
    Called at the end of :meth:`DataPipeline.execute`

    :param pipeline: The pipeline which was just executed
    :return: None
    """
    pass


def on_begin_execute_operation(operation: "DataOperation") -> None:
    """
    Called at the beginning of :meth:`DataOperation.execute`

    :param operation: The operation which is about to be executed
    :return: None
    """
    pass


def on_end_execute_operation(operation: "DataOperation") -> None:
    """
    Called at the end of :meth:`DataOperation.execute`

    :param operation: The operation which was just executed
    :return: None
    """
    pass


def on_begin_load_source(source: "DataSource") -> None:
    """
    Called at the beginning of :meth:`DataSource._load`, which
    is usually called when loading :paramref:`.DataSource.df`
    from either location or pipeline.

    :param source: DataSource for which DataFrame is about to be loaded
    :return:
    """
    pass


def on_end_load_source(source: "DataSource", df: pd.DataFrame) -> pd.DataFrame:
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


def on_begin_apply_variable_transform(
    transform: "Transform", source: "DataSource", col: "Column", var: "Variable"
) -> Tuple["DataSource", "Column", "Variable"]:
    """
    Called at the beginning of :meth:`Transform._apply_transform_for_column_and_variable_to_source`,
    which is called while transforming variables.

    The DataSource, Column, and Variable returned from this function will be used
    to do the variable transform

    :param transform: Transform which is about to be applied
    :param source: DataSource in which the transform is about to be applied
    :param col: Column on which the transform is about to be applied
    :param var: Variable on which the transform is about to be applied
    :return: The items which will be used to transform the variable
    """
    return source, col, var


def on_end_apply_variable_transform(
    transform: "Transform", source: "DataSource", col: "Column", var: "Variable"
) -> "DataSource":
    """
    Called at the end of :meth:`Transform._apply_transform_for_column_and_variable_to_source`,
    which is called while transforming variables.

    The DataSource returned from this function will be used
    as the results of the variable transform

    :param transform: Transform which was just applied
    :param source: DataSource in which the transform was just applied
    :param col: Column on which the transform was just applied
    :param var: Variable on which the transform was just applied
    :return: The DataSource which will be used as the results
        of the variable transform
    """
    return source


def on_begin_apply_source_transform(transform: 'SourceTransform', source: 'DataSource') -> 'DataSource':
    """
    Called at the beginning of :meth:`SourceTransform.apply`, which
    is called in TransformPipeline

    The DataSource returned from this function will be used
    to do the source transform

    :param transform: SourceTransform which is about to be applied
    :param source: DataSource on which the SourceTransform will be applied
    :return: The DataSource on which to apply the source transform
    """
    return source


def on_end_apply_source_transform(transform: 'SourceTransform', source: 'DataSource') -> 'DataSource':
    """
    Called at the end of :meth:`SourceTransform.apply`, which
    is called in TransformPipeline

    The DataSource returned from this function will be used
    as the result of the source transform

    :param transform: SourceTransform which was just applied
    :param source: DataSource on which the SourceTransform was just applied
    :return: The DataSource which will become the result of
        the source transformation
    """
    return source


_new_locals = {key: value for key, value in locals().items() if not key.startswith("_")}
_hook_keys = [key for key in _new_locals if key not in _orig_locals]
_orig_hooks = deepcopy(
    {key: value for key, value in _new_locals.items() if key in _hook_keys}
)


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


__all__ = _hook_keys + ["reset_hooks"]
