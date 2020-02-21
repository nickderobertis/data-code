import datetime
from copy import deepcopy
from functools import partial
from typing import Union, Sequence, List, Callable

from pyfileconf.basemodels.pipeline import Pipeline
from pyfileconf.selector.models.itemview import _is_item_view, ItemView

from datacode.models.source import DataSource
from datacode.models.merge import MergeOptions, LastMergeFinishedException, DataMerge

DataSourceOrPipeline = Union[DataSource, 'DataMergePipeline']
DataSourcesOrPipelines = Sequence[DataSourceOrPipeline]
MergeOptionsList = Sequence[MergeOptions]
DataMerges = List[DataMerge]


class DataPipeline:

    def __init__(self, data_sources: DataSourcesOrPipelines=None, merge_options_list: MergeOptionsList=None,
                 outpath=None, post_merge_cleanup_func=None, name: str=None, cleanup_kwargs: dict=None):

        if cleanup_kwargs is None:
            cleanup_kwargs = {}

        self.data_sources = data_sources
        self.merge_options_list = merge_options_list
        self._merge_index = 0
        self._set_cleanup_func(post_merge_cleanup_func, **cleanup_kwargs)
        self.outpath = outpath
        self.name = name
        self.cleanup_kwargs = cleanup_kwargs
        self.df = None

    def execute(self):
        pass

    def output(self, outpath=None):
        if outpath:
            self._output(outpath)
        elif self.outpath:
            self._output(self.outpath)

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        pass

    def _output(self, outpath=None):
        if self.df is not None:
            self.df.to_csv(outpath, index=False, encoding='utf8')

    # Following properties exist to recreate merges if data sources or merge options are overridden
    # by user

    @property
    def data_sources(self):
        return self._data_sources

    @data_sources.setter
    def data_sources(self, data_sources: DataSourcesOrPipelines):
        self._data_sources = data_sources

    @property
    def last_modified(self):
        self._touch_data_sources()
        return max([source.last_modified for source in self.data_sources])

    @property
    def source_last_modified(self):
        most_recent_time = datetime.datetime(1900, 1, 1)
        for i, source in enumerate(self.data_sources):
            if source.last_modified > most_recent_time:
                most_recent_time = source.last_modified
                most_recent_index = i

        return self.data_sources[most_recent_index]

    def copy(self):
        return deepcopy(self)

    def _touch_data_sources(self):
        """
        Data sources may be passed using pyfileconf.Selector, in which case they
        are pyfileconf.selector.models.itemview.ItemView objects. _get_merges uses isinstance, which will
        return ItemView, and so won't work correctly. By accessing the .item proprty of the ItemView,
        we get the original item back
        Returns:

        """
        # TODO: remove direct relationship with pyfileconf in data pipeline
        #
        # This needs to be handled better in the pyfileconf library first
        real_data_sources = []
        for data_source_or_view in self.data_sources:
            if _is_item_view(data_source_or_view):
                data_source_or_view: ItemView
                real_data_sources.append(data_source_or_view.item)
            else:
                real_data_sources.append(data_source_or_view)
        self.data_sources = real_data_sources

