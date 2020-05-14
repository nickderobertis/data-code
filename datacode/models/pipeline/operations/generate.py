import datetime
from typing import Callable, Optional, Any, Dict

from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource


class GenerationOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and outputs a DataSource
    """
    num_required_sources = 0
    options: 'GenerationOptions'
    result: 'DataSource'

    def __init__(self, options: 'GenerationOptions', **result_kwargs):
        super().__init__(
            [],
            options,
            **result_kwargs
        )

    def execute(self):
        ds = self.options.func(**self.options.func_kwargs)
        self.result.update_from_source(ds)
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        # TODO [#46]: better summary for DataGeneratorPipeline
        print(f'Calling function {self.options.func.__name__} with kwargs {self.options.func_kwargs} to '
              f'generate a DataSource')

    def describe(self):
        return self.summary()

    def __repr__(self):
        return f'<GenerationOperation(options={self.options})>'


class GenerationOptions(OperationOptions):
    """
    Class for options passed to AnalysisOperations
    """
    op_class = GenerationOperation

    def __init__(self, func: Callable[[Any], DataSource], last_modified: Optional[datetime.datetime] = None,
                 out_path: Optional[str] = None, allow_modifying_result: bool = True,
                 result_kwargs: Optional[Dict[str, Any]] = None,
                 **func_kwargs):
        """

        :param func: function which generates the DataSource
        :param last_modified: by default, will always run pipeline when loading generated source, but by passing a
            last_modified can avoid re-running and instead load from location
        :param out_path: location for generated DataSource
        :param func_kwargs: Keyword arguments to pass to the function which generates the DataSource
        :param allow_modifying_result: When DataSources are directly linked to pipelines, loading
            source from pipeline can cause modifications in the pipeline's result source. Set to False
            to ensure it won't be modified (but uses more memory). Setting to False should only be needed
            if multiple sources load from the same pipeline in one session
        """
        self.func = func
        self.func_kwargs = func_kwargs
        self.last_modified = last_modified
        self.out_path = out_path
        self.allow_modifying_result = allow_modifying_result
        self.result_kwargs = result_kwargs
