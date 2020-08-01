import datetime
from typing import Callable, Optional, Any, Union, Dict, Sequence, TYPE_CHECKING

from datacode.logger import logger

if TYPE_CHECKING:
    from datacode.models.pipeline.transform import DataTransformationPipeline

from datacode.models.transform.source import SourceTransform
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourcesOrPipelines
from datacode.models.variables.variable import Variable


class TransformOperation(DataOperation):
    """
    Data operation that takes one DataSource as an input and outputs a DataSource
    """
    options: 'TransformOptions'
    result: 'DataSource'

    def __init__(self, pipeline: 'DataTransformationPipeline', data_sources: DataSourcesOrPipelines,
                 options: 'TransformOptions', **result_kwargs):
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
        ds = self.options.transform.apply(self.data_source, preserve_original=self.options.preserve_original)
        self.result.update_from_source(ds, exclude_attrs=('location', 'data_outputter_kwargs', 'pipeline'))
        return self.result

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        # TODO [#53]: better summary for DataTransformationPipeline
        logger.info(f'Calls transform {self.options.transform} on existing '
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
    repr_cols = [
        'func',
        'out_path',
        'allow_modifying_result',
        'preserve_original',
    ]

    def __init__(self, func: Union[Callable[[DataSource, Any], DataSource], SourceTransform],
                 preserve_original: bool = True, out_path: Optional[str] = None,
                 allow_modifying_result: bool = True, result_kwargs: Optional[Dict[str, Any]] = None,
                 transform_key: Optional[str] = None, always_rerun: bool = False,
                 last_modified: Optional[datetime.datetime] = None,
                 subset: Optional[Union[
                     Sequence[Variable],
                     Callable[[DataSource], Sequence[Variable]]
                 ]] = None):
        """

        :param func:
        :param preserve_original:
        :param out_path:
        :param allow_modifying_result: When DataSources are directly linked to pipelines, loading
            source from pipeline can cause modifications in the pipeline's result source. Set to False
            to ensure it won't be modified (but uses more memory). Setting to False should only be needed
            if multiple sources load from the same pipeline in one session
        :param transform_key: Only used when passing callable instead of SourceTransform. Sets the key for the
            generated SourceTransform
        :param always_rerun: Whether to re-run operation if executed multiple times
        :param last_modified: manually override last modified
        :param subset: Only applies when function is passed rather than SourceTransform. See SourceTransform.subset
        """
        if isinstance(func, SourceTransform):
            self.transform = func
        else:
            self.transform = SourceTransform.from_func(func, key=transform_key, subset=subset)

        self.func = func
        self.preserve_original = preserve_original
        self.out_path = out_path
        self.allow_modifying_result = allow_modifying_result
        self.result_kwargs = result_kwargs
        self.always_rerun = always_rerun
        self.last_modified = last_modified
