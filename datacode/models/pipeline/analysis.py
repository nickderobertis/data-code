import datetime
from typing import Any, Optional, Callable

from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline


class DataAnalysisPipeline(DataPipeline):

    def __init__(self, data_source: DataSource,
                 func: Callable[[DataSource, Any], Any], name: Optional[str] = None,
                 **func_kwargs):
        self.func = func
        self.func_kwargs = func_kwargs
        super().__init__(data_sources=[data_source], name=name)

    @property
    def data_source(self) -> DataSource:
        return self.data_sources[0]

    def execute(self) -> Any:
        self.result = self.func(self.data_source, **self.func_kwargs)
        return self.result

    def summary(self, **kwargs):
        # TODO: better summary for DataAnalysisPipeline
        print(f'Runs func {self.func.__name__} with kwargs {self.func_kwargs} on {self.data_source}:')
        print(f'{self.data_source.describe()}')