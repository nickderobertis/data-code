from copy import deepcopy
import pandas as pd
from functools import partial
import os
import warnings
import datetime
from typing import Callable, TYPE_CHECKING, List, Optional, Any, Dict, Sequence, Type

from pd_utils.optimize.load import read_file as read_file_into_df

from datacode.models.column.column import Column
from datacode.models.loader import DataLoader
from datacode.models.type import DataType

if TYPE_CHECKING:
    from datacode.models.pipeline import DataPipeline

class DataSource:

    def __init__(self, location: Optional[str] = None, df: Optional[pd.DataFrame] = None,
                 pipeline: Optional['DataPipeline'] = None, columns: Optional[Dict[str, Column]] = None,
                 name: Optional[str] = None, data_type: Optional[str] = None, tags: Optional[List[str]] = None,
                 loader_class: Optional[Type[DataLoader]] = None, read_file_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):

        if read_file_kwargs is None:
            read_file_kwargs = {}
        if loader_class is None:
            loader_class = DataLoader

        self._check_inputs(location, df)
        self.location = location
        self.name = name
        self.data_type = data_type
        self.tags = tags # TODO: better handling for tags
        self.loader_class = loader_class
        self.pipeline = pipeline
        self.columns = columns
        self.read_file_kwargs = read_file_kwargs
        self.optimize_size = optimize_size
        self._df = df
        self.name_type = f'{name} {self.data_type}'

    @property
    def df(self):
        if self._df is None:
            self._df = self._load()
        return self._df

    @df.setter
    def df(self, df):
        self._df = df

    @property
    def data_type(self):
        return self._type

    @data_type.setter
    def data_type(self, dtype):
        self._type = DataType(dtype)

    @property
    def last_modified(self):
        if self.location is None:
            # No location. Setting last modified time as a long time ago, so will trigger pipeline instead
            return datetime.datetime(1899, 1, 1)

        return datetime.datetime.fromtimestamp(os.path.getmtime(self.location))

    def _load(self):
        self._touch_pipeline()
        if not hasattr(self, 'data_loader'):
            self._set_data_loader(self.loader_class, pipeline=self.pipeline, **self.read_file_kwargs)
        return self.data_loader()

    def output(self, **to_csv_kwargs):
        self.df.to_csv(self.location, **to_csv_kwargs)

    def _check_inputs(self, filepath, df):
        pass
        # assert not (filepath is None) and (df is None)

    def _set_data_loader(self, data_loader_class: Type[DataLoader], pipeline: 'DataPipeline' =None, **read_file_kwargs):
        run_pipeline = False
        if pipeline is not None:
            # if a source in the pipeline to create this data source was modified more recently than this data source
            # note: if there is no location, will always enter the next block, as last modified time will set
            # to a long time ago
            if pipeline.last_modified > self.last_modified:
                # a prior source used to construct this data source has changed. need to re run pipeline
                recent_source = pipeline.source_last_modified
                warnings.warn(f'''data source {recent_source} was modified at {recent_source.last_modified}.

                this data source {self} was modified at {self.last_modified}.

                to get new changes, will load this data source through pipeline rather than from file.''')

                run_pipeline = True
            # otherwise, don't need to worry about pipeline, continue handling

        loader = data_loader_class(self, read_file_kwargs, self.optimize_size)

        # If necessary, run pipeline before loading
        # Still necessary to use loader as may be transforming the data
        if run_pipeline:
            def run_pipeline_then_load(pipeline):
                pipeline.execute() # outputs to file
                return loader.load()
            self.data_loader = partial(run_pipeline_then_load, self.pipeline)
        else:
            self.data_loader = loader.load

    def _touch_pipeline(self):
        """
        Pipeline may be passed using pyfileconf.Selector, in which case it is
        a pyfileconf.selector.models.itemview.ItemView object. _set_data_loader accesses a property of
        the pipeline before it's configured, and so won't work correctly. By accessing the .item proprty of the ItemView,
        we get the original item back
        Returns:

        """
        from pyfileconf.selector.models.itemview import _is_item_view

        if _is_item_view(self.pipeline):
            self.pipeline = self.pipeline.item

    def copy(self):
        return deepcopy(self)

    def __repr__(self):
        return f'<DataSource(name={self.name}, type={self.data_type.name})>'