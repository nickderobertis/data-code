from typing import Union, Sequence, TYPE_CHECKING

from typing_extensions import Protocol, runtime_checkable

if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.pipeline.base import DataPipeline
    from datacode.models.pipeline.merge import DataMergePipeline
    from datacode.models.pipeline.generate import DataGeneratorPipeline
    from datacode.models.pipeline.transform import DataTransformationPipeline
    from datacode.models.pipeline.operations.operation import OperationOptions, DataOperation

SourceCreatingPipeline = Union['DataMergePipeline', 'DataGeneratorPipeline', 'DataTransformationPipeline']
DataSourceOrPipeline = Union['DataSource', SourceCreatingPipeline]
DataSourcesOrPipelines = Sequence[DataSourceOrPipeline]
ObjWithLastModified = Union[DataSourceOrPipeline, 'OperationOptions', 'DataOperation']


@runtime_checkable
class HasDifficulty(Protocol):
    difficulty: float


@runtime_checkable
class HasDataSources(Protocol):
    data_sources: DataSourcesOrPipelines


@runtime_checkable
class HasPipeline(Protocol):
    pipeline: 'DataPipeline'
