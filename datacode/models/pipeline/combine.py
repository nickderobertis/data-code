from typing import Optional, Sequence

from datacode.models.pipeline.base import DataPipeline
from datacode.models.pipeline.operations.combine import CombineOptions
from datacode.models.types import DataSourcesOrPipelines


class DataCombinationPipeline(DataPipeline):
    """
    A DataPipeline which combines data from two or more DataSources
    """

    def __init__(self, data_sources: DataSourcesOrPipelines,
                 options_list: Sequence[CombineOptions], name: Optional[str] = None,
                 difficulty: float = 50):
        super().__init__(
            data_sources,
            options_list,
            name=name,
            difficulty=difficulty
        )

    def execute(self, output: bool = True):
        self._validate()
        super().execute(output=output)

    def _validate(self):
        self._validate_data_sources_combine_options()

    def _validate_data_sources_combine_options(self):
        if len(self.data_sources) - 1 != len(self.operation_options):
            raise ValueError(f'must have one fewer combine options than data sources. Have {len(self.data_sources)} data '
                             f'sources and {len(self.operation_options)} combine options.')
