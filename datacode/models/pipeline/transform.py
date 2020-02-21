from typing import Callable, Optional, Any, Union

from datacode.models.transform.source import SourceTransform
from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline


class DataTransformationPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource directly from a single other DataSource
    """

    def __init__(self, data_source: DataSource, func: Union[Callable[[DataSource, Any], DataSource], SourceTransform],
                 preserve_original: bool = True, outpath: Optional[str] = None, name: Optional[str] = None):
        if isinstance(func, SourceTransform):
            self.transform = func
        else:
            self.transform = SourceTransform.from_func(func)
        self.func = func
        self.preserve_original = preserve_original
        super().__init__(data_sources=[data_source], outpath=outpath, name=name)

    def execute(self):
        ds = self.transform.apply(self.data_sources[0], preserve_original=self.preserve_original)
        self.data_sources = [self.data_sources[0], ds]
        self.df = ds.df
        self.output()

    def summary(self, **kwargs):
        # TODO [#53]: better summary for DataTransformationPipeline
        print(f'Pipeline {self.name} calls function {self.func.__name__} on existing '
              f'data source {self.data_sources[0]}')





