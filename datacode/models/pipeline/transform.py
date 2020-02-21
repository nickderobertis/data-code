import datetime
import uuid
from typing import Callable, Optional, Any, Union

from datacode.models.variables.transform.transform import Transform, AppliedTransform
from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline


class DataTransformationPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource directly from a single other DataSource
    """

    def __init__(self, data_source: DataSource, func: Union[Callable[[DataSource, Any], DataSource], Transform],
                 func_target: str = 'source', preserve_original: bool = True,
                 outpath: Optional[str] = None,
                 name: Optional[str] = None, last_modified: Optional[datetime.datetime] = None,
                 **func_kwargs):
        if isinstance(func, Transform):
            self.transform = func
        else:
            self.transform = AppliedTransform.from_func(func, data_func_target=func_target, **func_kwargs)
        self.func = func
        self.func_kwargs = func_kwargs
        self.preserve_original = preserve_original
        super().__init__(data_sources=[data_source], outpath=outpath, name=name, last_modified=last_modified)

    def execute(self):
        ds = self.transform.apply_to_source(self.data_sources[0], preserve_original=self.preserve_original)
        self.data_sources = [self.data_sources[0], ds]
        self.df = ds.df
        self.output()

    def summary(self, **kwargs):
        # TODO: better summary for DataTransformationPipeline
        print(f'Pipeline {self.name} calls function {self.func.__name__} with kwargs {self.func_kwargs} on existing '
              f'data source {self.data_sources[0]}')





