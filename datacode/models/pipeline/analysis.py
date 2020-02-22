import datetime
from typing import Any, Optional, Callable

from datacode.models.pipeline.operations.analysis import AnalysisOptions
from datacode.models.source import DataSource

from datacode.models.pipeline.base import DataPipeline
from datacode.models.types import DataSourceOrPipeline


class DataAnalysisPipeline(DataPipeline):
    """
    A DataPipeline which starts from a single data source but does not produce a data source
    """

    def __init__(self, data_source: DataSourceOrPipeline, options: AnalysisOptions,
                 name: Optional[str] = None):
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
