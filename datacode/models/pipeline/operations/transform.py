from typing import Callable, Optional, Any, Union

from datacode.models.transform.source import SourceTransform
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourcesOrPipelines


class TransformOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and outputs a DataSource
    """
    options: 'TransformOptions'
    result: 'DataSource'

    def __init__(self, data_sources: DataSourcesOrPipelines, options: 'TransformOptions'):
        super().__init__(
            data_sources,
            options
        )

    @property
    def data_source(self) -> DataSource:
        return self.data_sources[0]

    def execute(self):
        ds = self.options.transform.apply(self.data_source, preserve_original=self.options.preserve_original)
        self.result.update_from_source(ds)
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        # TODO [#53]: better summary for DataTransformationPipeline
        print(f'Calls transform {self.options.transform} on existing '
              f'data source {self.data_source}')

    def describe(self):
        return self.summary()

    def __repr__(self):
        return f'<TransformOperation(data_source={self.data_source}, options={self.options})>'

    def _validate(self):
        self._validate_data_sources_and_options()

    def _validate_data_sources_and_options(self):
        if len(self.data_sources) > 1:
            raise ValueError(f'got more than one data source got DataTransformPipeline, not clear which to use. '
                             f'Data sources: {self.data_sources}')


class TransformOptions(OperationOptions):
    """
    Class for options passed to AnalysisOperations
    """
    op_class = TransformOperation

    def __init__(self, func: Union[Callable[[DataSource, Any], DataSource], SourceTransform],
                 preserve_original: bool = True, out_path: Optional[str] = None):
        if isinstance(func, SourceTransform):
            self.transform = func
        else:
            self.transform = SourceTransform.from_func(func)

        self.func = func
        self.preserve_original = preserve_original
        self.out_path = out_path
