from functools import partial
from typing import Callable, Any, Dict, Optional, Sequence

from datacode.models.pipeline.operations.merge import MergeOptions
from datacode.models.pipeline.base import DataPipeline
from datacode.models.types import DataSourcesOrPipelines


class DataMergePipeline(DataPipeline):
    """
    Handles data pipelines involving merges between two or more sources or pipelines
    """

    def __init__(self, data_sources: DataSourcesOrPipelines = None,
                 merge_options_list: Optional[Sequence[MergeOptions]] = None,
                 post_merge_cleanup_func: Optional[Callable] = None,
                 name: Optional[str] = None, cleanup_kwargs: Optional[Dict[str, Any]] = None):

        if cleanup_kwargs is None:
            cleanup_kwargs = {}
        if merge_options_list is None:
            merge_options_list = []
        if data_sources is None:
            data_sources = []

        self._set_cleanup_func(post_merge_cleanup_func, **cleanup_kwargs)
        self.cleanup_kwargs = cleanup_kwargs
        self.data_sources = data_sources

        super().__init__(data_sources, merge_options_list, name=name)

    def _validate(self):
        self._validate_data_sources_merge_options()

    def _validate_data_sources_merge_options(self):
        if len(self.data_sources) - 1 != len(self.operation_options):
            raise ValueError(f'must have one fewer merge options than data sources. Have {len(self.data_sources)} data '
                             f'sources and {len(self.operation_options)} merge options.')

    def execute(self):
        self._validate()

        super().execute(output=False)

        if self.has_post_merge_cleanup_func:
            self.df = self.post_merge_cleanup_func(self.df)
        self.output()

        return self.df

    def _set_cleanup_func(self, post_merge_cleanup_func, **cleanup_kwargs):
        if post_merge_cleanup_func is not None:
            self.has_post_merge_cleanup_func = True
            self.post_merge_cleanup_func = partial(post_merge_cleanup_func, **cleanup_kwargs)
        else:
            self.has_post_merge_cleanup_func = False

