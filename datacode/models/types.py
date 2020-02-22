from typing import Union, Sequence, TYPE_CHECKING
if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.pipeline.merge import DataMergePipeline
    from datacode.models.pipeline.generate import DataGeneratorPipeline
    from datacode.models.pipeline.transform import DataTransformationPipeline
    from datacode.models.pipeline.operations.operation import OperationOptions

SourceCreatingPipeline = Union['DataMergePipeline', 'DataGeneratorPipeline', 'DataTransformationPipeline']
DataSourceOrPipeline = Union['DataSource', SourceCreatingPipeline]
DataSourcesOrPipelines = Sequence[DataSourceOrPipeline]
ObjWithLastModified = Union[DataSourceOrPipeline, 'OperationOptions']