import datetime
from typing import Callable, Optional, Any, Sequence, Dict

from datacode.models.logic.combine import CombineFunction, combine_sources
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource


class CombineOperation(DataOperation):
    """
    Data operation that combines data from two DataSources
    """
    num_required_sources = 2
    options: 'CombineOptions'
    result: 'DataSource'

    def __init__(self, data_sources: Sequence[DataSource], options: 'CombineOptions', **result_kwargs):
        super().__init__(
            data_sources,
            options,
            **result_kwargs
        )
        self.options.last_modified = max([source.last_modified for source in self.data_sources if source.last_modified])

    def execute(self):
        ds = self.options.func(self.data_sources, **self.options.func_kwargs)
        self.result.update_from_source(ds)
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        # TODO [#73]: better summary for DataCombinationPipeline
        print(f'Combining {self.data_sources[0]} with {self.data_sources[1]} with '
              f'options {self.options}')

    def describe(self):
        return self.summary()

    def __repr__(self):
        return f'<CombinationOperation(options={self.options})>'


class CombineOptions(OperationOptions):
    """
    Class for options passed to AnalysisOperations
    """
    op_class = CombineOperation

    def __init__(self, func: CombineFunction = combine_sources,
                 out_path: Optional[str] = None,
                 result_kwargs: Optional[Dict[str, Any]] = None,
                 **func_kwargs):
        """

        :param func: function which combines the DataSources
        :param out_path: location for generated DataSource
        :param func_kwargs: Keyword arguments to pass to the function which generates the DataSource
        """
        self.func = func
        self.func_kwargs = func_kwargs
        self.out_path = out_path
        self.result_kwargs = result_kwargs
