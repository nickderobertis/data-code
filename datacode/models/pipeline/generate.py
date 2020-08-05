from typing import Optional

from datacode.models.pipeline.operations.generate import GenerationOptions

from datacode.models.pipeline.base import DataPipeline


class DataGeneratorPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource without using any other DataSource
    """

    def __init__(self, options: GenerationOptions, name: Optional[str] = None, difficulty: float = 50,
                 auto_cache: bool = True,
                 auto_cache_location: Optional[str] = None, cache_key: str = ''):
        super().__init__(
            [],
            [options],
            name=name,
            difficulty=difficulty,
            auto_cache=auto_cache,
            auto_cache_location=auto_cache_location,
            cache_key=cache_key,
        )
