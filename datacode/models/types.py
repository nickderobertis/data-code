from typing import Union, Sequence, List, TYPE_CHECKING
if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.merge import DataMerge
    from datacode.models.merge import MergeOptions
    from datacode.models.pipeline.merge import DataMergePipeline
    from datacode.models.pipeline.generate import DataGeneratorPipeline
    from datacode.models.pipeline.transform import DataTransformationPipeline

SourceCreatingPipeline = Union['DataMergePipeline', 'DataGeneratorPipeline', 'DataTransformationPipeline']
DataSourceOrPipeline = Union['DataSource', SourceCreatingPipeline]
DataSourcesOrPipelines = Sequence[DataSourceOrPipeline]
MergeOptionsList = Sequence['MergeOptions']
DataMerges = List['DataMerge']