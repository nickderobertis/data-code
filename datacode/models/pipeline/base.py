import datetime
from copy import deepcopy
from typing import Union, Sequence, List, Callable, Optional

from datacode.models.source import DataSource
from datacode.models.merge import MergeOptions, DataMerge

DataSourceOrPipeline = Union[DataSource, 'DataMergePipeline']
DataSourcesOrPipelines = Sequence[DataSourceOrPipeline]
MergeOptionsList = Sequence[MergeOptions]
DataMerges = List[DataMerge]


class DataPipeline:
    """
    Base class for data pipelines. Should not be used directly.
    """

    def __init__(self, data_sources: DataSourcesOrPipelines = None, outpath: Optional[str] = None,
                 name: Optional[str] = None, last_modified: Optional[datetime.datetime] = None):
        self.data_sources = data_sources
        self.outpath = outpath
        self.name = name
        self.df = None
        self._manual_last_modified = last_modified
        self.result = None

    def execute(self):
        raise NotImplementedError('child class must implement execute')

    def output(self, outpath=None):
        if outpath:
            self._output(outpath)
        elif self.outpath:
            self._output(self.outpath)

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        raise NotImplementedError('child class must implement summary')

    def _output(self, outpath=None):
        if self.df is not None:
            self.df.to_csv(outpath, index=False, encoding='utf8')

    @property
    def data_sources(self):
        return self._data_sources

    @data_sources.setter
    def data_sources(self, data_sources: DataSourcesOrPipelines):
        self._data_sources = data_sources

    @property
    def last_modified(self) -> Optional[datetime.datetime]:
        if self._manual_last_modified is not None:
            return self._manual_last_modified
        if self.data_sources is None:
            return None
        return max([source.last_modified for source in self.data_sources])

    @property
    def source_last_modified(self) -> Optional[DataSource]:
        if self.data_sources is None:
            return None
        most_recent_time = datetime.datetime(1900, 1, 1)
        most_recent_index = None
        for i, source in enumerate(self.data_sources):
            if source.last_modified > most_recent_time:
                most_recent_time = source.last_modified
                most_recent_index = i

        if most_recent_index is not None:
            return self.data_sources[most_recent_index]

    def copy(self):
        return deepcopy(self)
