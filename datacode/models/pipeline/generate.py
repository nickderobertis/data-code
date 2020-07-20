from typing import Optional

from datacode.models.pipeline.operations.generate import GenerationOptions

from datacode.models.pipeline.base import DataPipeline


class DataGeneratorPipeline(DataPipeline):
    """
    A DataPipeline which creates a DataSource without using any other DataSource
    """

    def __init__(self, options: GenerationOptions, name: Optional[str] = None, difficulty: float = 50):
        super().__init__(
            [],
            [options],
            name=name,
            difficulty=difficulty
        )
