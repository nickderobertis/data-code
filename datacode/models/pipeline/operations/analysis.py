from typing import Sequence, Callable, Optional, Any, Dict

from datacode.models.analysis import AnalysisResult
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourceOrPipeline, DataSourcesOrPipelines


class AnalysisOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and does not output a DataSource
    """

    def __init__(self, data_sources: DataSourcesOrPipelines, options: 'AnalysisOptions', **result_kwargs):
        super().__init__(
            data_sources,
            options,
            **result_kwargs
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


def analysis_result_to_file(result: AnalysisResult, out_path: str):
    with open(out_path, 'w') as f:
        f.write(str(result.result))


class AnalysisOptions(OperationOptions):
    """
    Class for options passed to AnalysisOperations
    """
    op_class = AnalysisOperation
    result_class = AnalysisResult

    def __init__(self, func: Callable[[DataSource, Any], Any],
                 output_func: Optional[Callable[['AnalysisResult', str], None]] = analysis_result_to_file,
                 out_path: Optional[str] = None, result_kwargs: Optional[Dict[str, Any]] = None,
                 **func_kwargs):
        self.func = func
        self.func_kwargs = func_kwargs
        self.analysis_output_func = output_func
        self.out_path = out_path
        self.result_kwargs = result_kwargs

    @property
    def can_output(self) -> bool:
        return self.out_path is not None and self.analysis_output_func is not None
