"""
Hooks to run functions during datacode operations globally

:Usage:

    >>> import datacode.hooks as dc_hooks
    >>>
    >>> def log_begin_execute(pipeline: DataPipeline):
    >>>     print(f"running {pipeline.name}")
    >>>
    >>> dc_hooks.on_begin_execute_pipeline = log_begin_execute

:All the Hooks:

    Each hook may expect different arguments and have different return values.
    Here is the list of the hooks, along with the expected arguments and return values:

    on_begin_execute_pipeline(pipeline: DataPipeline) -> None
    on_end_execute_pipeline(pipeline: DataPipeline) -> None
"""

def _null_hook(**kwargs):
    return kwargs

on_begin_execute_pipeline = _null_hook
on_end_execute_pipeline = _null_hook
