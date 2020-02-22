import datetime
from typing import Callable, Optional, Any

from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource


class GenerationOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and outputs a DataSource
    """
    num_required_sources = 0
    options: 'GenerationOptions'
    result: 'DataSource'

    def __init__(self, options: 'GenerationOptions'):
        super().__init__(
            [],
            options
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
                 out_path: Optional[str] = None, **func_kwargs):
        """

        :param func: function which generates the DataSource
        :param last_modified: by default, will always run pipeline when loading generated source, but by passing a
            last_modified can avoid re-running and instead load from location
        :param out_path: location for generated DataSource
        :param func_kwargs: Keyword arguments to pass to the function which generates the DataSource
        """
        self.func = func
        self.func_kwargs = func_kwargs
        self.last_modified = last_modified
        self.out_path = out_path
