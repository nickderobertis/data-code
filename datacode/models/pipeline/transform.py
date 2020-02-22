from typing import Optional

from datacode.models.pipeline.operations.transform import TransformOptions
from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline


class DataTransformationPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource directly from a single other DataSource
    """

    def __init__(self, data_source: DataSource, options: TransformOptions, name: Optional[str] = None):
        super().__init__(
            [data_source],
            [options],
            name=name
        )

    @property
    def data_source(self) -> DataSource:
        return self.data_sources[0]

    @data_source.setter
    def data_source(self, source: DataSource):
        self.data_sources[0] = source
