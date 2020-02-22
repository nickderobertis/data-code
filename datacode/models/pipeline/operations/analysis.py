from typing import Sequence, Callable, Optional, Any

from datacode.models.analysis import AnalysisResult
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourceOrPipeline, DataSourcesOrPipelines


class AnalysisOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and does not output a DataSource
    """

    def __init__(self, data_sources: DataSourcesOrPipelines, options: 'AnalysisOptions'):
        super().__init__(
            data_sources,
            options
        )

    @property
    def data_source(self) -> DataSource:
        return self.data_sources[0]

    def execute(self):
        self.result.result = self.options.func(self.data_source, **self.options.func_kwargs)
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        # TODO [#45]: better summary for DataAnalysisPipeline
        print(f'Runs func {self.options.func.__name__} with kwargs {self.options.func_kwargs} on {self.data_source}:')
        print(f'{self.data_source.describe()}')

    def describe(self):
        return self.summary()

    def __repr__(self):
        return f'<AnalysisOperation(data_source={self.data_source}, options={self.options})>'

    def _validate(self):
        self._validate_data_sources_and_options()

    def _validate_data_sources_and_options(self):
        if len(self.data_sources) > 1:
            raise ValueError(f'got more than one data source got DataAnalysisPipeline, not clear which to use. '
                             f'Data sources: {self.data_sources}')


class AnalysisOptions(OperationOptions):
    """
    Class for options passed to AnalysisOperations
    """
    op_class = AnalysisOperation
    result_class = AnalysisResult

    def __init__(self, func: Callable[[DataSource, Any], Any], **func_kwargs):
        self.func = func
        self.func_kwargs = func_kwargs
