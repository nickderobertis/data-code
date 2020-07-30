import datetime
from typing import Callable, Optional, Any, Dict, TYPE_CHECKING

from datacode.models.types import DataSourcesOrPipelines

if TYPE_CHECKING:
    from datacode.models.pipeline.base import DataPipeline

from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource


class LoadOperation(DataOperation):
    """
    Data operation that loads a DataSource from a location
    """
    options: 'LoadOptions'
    result: 'DataSource'

    def __init__(self, pipeline: 'DataPipeline', data_sources: DataSourcesOrPipelines,
                 options: 'LoadOptions',
                 **result_kwargs):
        super().__init__(
            pipeline,
            data_sources,
            options,
            **result_kwargs
        )

    @property
    def data_source(self) -> DataSource:
        return self.data_sources[0]

    def _execute(self):
        self.data_source.df  # causes load of df
        self.result.update_from_source(self.data_source)
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        print(f'Loading cached source {self.data_source}')

    def describe(self):
        return self.summary()

    def __repr__(self):
        return f'<LoadOperation(options={self.options})>'

    @property
    def last_modified(self) -> datetime.datetime:
        return self.data_source.last_modified

    def _set_result(self, **kwargs):
        # Override base implementation to not pass last_modified
        # as it should be taken from the data
        self.result = self.options.result_class(
            name=self.output_name,
            location=self.options.out_path,
            pipeline=self.pipeline,
            **kwargs
        )


class LoadOptions(OperationOptions):
    """
    Class for options passed to LoadOperations
    """
    op_class = LoadOperation

    def __init__(self,
                 out_path: Optional[str] = None, allow_modifying_result: bool = True,
                 result_kwargs: Optional[Dict[str, Any]] = None, always_rerun: bool = False):
        """

        :param out_path: location for generated DataSource
        :param allow_modifying_result: When DataSources are directly linked to pipelines, loading
            source from pipeline can cause modifications in the pipeline's result source. Set to False
            to ensure it won't be modified (but uses more memory). Setting to False should only be needed
            if multiple sources load from the same pipeline in one session
        :param always_rerun: Whether to re-run operation if executed multiple times
        """
        self.out_path = out_path
        self.allow_modifying_result = allow_modifying_result
        self.result_kwargs = result_kwargs
        self.always_rerun = always_rerun
