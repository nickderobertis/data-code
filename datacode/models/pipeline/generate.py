import datetime
from typing import Callable, Optional, Any

from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline


class DataGeneratorPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource without using any other DataSource
    """

    def __init__(self, func: Callable[[Any], DataSource], outpath: Optional[str] = None,
                 name: Optional[str] = None, last_modified: Optional[datetime.datetime] = None,
                 **func_kwargs):
        self.func = func
        self.func_kwargs = func_kwargs
        super().__init__(outpath=outpath, name=name, last_modified=last_modified)

    def execute(self):
        ds = self.func(**self.func_kwargs)
        self.data_sources = [ds]
        self.df = ds.df
        self.output()

    def summary(self, **kwargs):
        # TODO [#46]: better summary for DataGeneratorPipeline
        print(f'Pipeline {self.name} calls function {self.func.__name__} with kwargs {self.func_kwargs}')






