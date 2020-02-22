import datetime
from copy import deepcopy
from typing import Union, Sequence, List, Callable, Optional

from datacode.models.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourcesOrPipelines, DataSourceOrPipeline


class DataPipeline:
    """
    Base class for data pipelines. Should not be used directly.
    """

    def __init__(self, data_sources: DataSourcesOrPipelines,
                 operation_options: Optional[Sequence[OperationOptions]],
                 outpath: Optional[str] = None,
                 name: Optional[str] = None, last_modified: Optional[datetime.datetime] = None):
        if operation_options is None:
            operation_options = []

        self.data_sources = data_sources
        self.operation_options = operation_options
        self.outpath = outpath
        self.name = name
        self.df = None
        self._manual_last_modified = last_modified
        self._operation_index = 0
        self.result = None

    def execute(self, output: bool = True):
        while True:
            try:
                self.next_operation()
            except LastOperationFinishedException:
                break

        if output:
            self.output()

        return self.df

    def next_operation(self):
        if self._operation_index == 0:
            self._set_df_from_first_operation()

        self._do_operation()

    def _do_operation(self):
        try:
            operation = self.operations[self._operation_index]
            print(f'Now running operation {self._operation_index + 1}: {operation}')
        except IndexError:
            raise LastOperationFinishedException

        operation.execute()

        # Set current df to result of merge
        self.df = operation.result.df

        self._operation_index += 1

    def _set_operations(self):
        self._operations = self._create_operations(self.data_sources, self.operation_options)

    def _create_operations(self, data_sources: DataSourcesOrPipelines, options_list: List[OperationOptions]):
        operations = _get_operations(data_sources[0], data_sources[1], options_list[0])
        if len(options_list) == 1:
            return operations

        for i, options in enumerate(options_list[1:]):
            operations += _get_operations(operations[-1].result, data_sources[i + 2], options)

        return operations

    def _set_df_from_first_operation(self):
        self.df = self.operations[0].data_sources[0].df

    @property
    def operations(self):
        try:
            return self._operations
        except AttributeError:
            self._set_operations()

        return self._operations

    # Following properties exist to recreate operations if data sources or merge options are overridden
    # by user

    @property
    def data_sources(self):
        return self._data_sources

    @data_sources.setter
    def data_sources(self, data_sources: DataSourcesOrPipelines):
        self._data_sources = data_sources
        # only set merges if previously set. otherwise no need to worry about updating cached result
        if hasattr(self, '_operations'):
            self._set_operations()

    @property
    def operation_options(self):
        return self._operations_options

    @operation_options.setter
    def operation_options(self, options: List[OperationOptions]):
        self._operations_options = options
        # only set merges if previously set. otherwise no need to worry about updating cached result
        if hasattr(self, '_operations'):
            self._set_operations()

    def output(self, outpath=None):
        if outpath:
            self._output(outpath)
        elif self.outpath:
            self._output(self.outpath)

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        for op in self.operations:
            op.summary(
                *summary_args,
                summary_method=summary_method,
                summary_function=summary_function,
                summary_attr=summary_attr,
                **summary_method_kwargs
            )

    def describe(self):
        for op in self.operations:
            op.describe()

    def _output(self, outpath=None):
        if self.df is not None:
            self.df.to_csv(outpath, index=False, encoding='utf8')

    @property
    def data_sources(self):
        return self._data_sources

    @data_sources.setter
    def data_sources(self, data_sources: DataSourcesOrPipelines):
        self._data_sources = data_sources

    @property
    def last_modified(self) -> Optional[datetime.datetime]:
        if self._manual_last_modified is not None:
            return self._manual_last_modified
        if self.data_sources is None:
            return None
        return max([source.last_modified for source in self.data_sources])

    @property
    def source_last_modified(self) -> Optional[DataSource]:
        if self.data_sources is None:
            return None
        most_recent_time = datetime.datetime(1900, 1, 1)
        most_recent_index = None
        for i, source in enumerate(self.data_sources):
            if source.last_modified > most_recent_time:
                most_recent_time = source.last_modified
                most_recent_index = i

        if most_recent_index is not None:
            return self.data_sources[most_recent_index]

    def copy(self):
        return deepcopy(self)


class LastOperationFinishedException(Exception):
    pass


def _get_operations(data_source_1: DataSourceOrPipeline, data_source_2: DataSourceOrPipeline,
                    options: OperationOptions) -> List[DataOperation]:
    """
    Creates a list of DataOperation/subclass objects from a paring of two DataSource objects, a DataSource and a
    DataPipeline, or two DataPipeline objects.
    :param data_source_1: DataSource or DataMergePipeline
    :param data_source_2: DataSource or DataMergePipeline
    :param options: Options for the operation
    :return: list of DataOperation/subclass objects
    """
    # TODO: work DataTransformationPipeline and DataGenerationPipeline into merge creation in DataMergePipeline
    operations: List[DataOperation] = []
    final_operation_sources: List[DataSource] = []
    # Add any pipeline operations first, as the results from the pipeline must be ready before we can use the results
    # for other data sources or pipeline results operations
    if _is_data_pipeline(data_source_1):
        operations += data_source_1.operations  # type: ignore
        pipeline_1_result = data_source_1.operations[-1].result  # type: ignore
        # result of first pipeline will be first source in final operation
        final_operation_sources.append(pipeline_1_result)
    else:
        final_operation_sources.append(data_source_1)  # type: ignore

    if _is_data_pipeline(data_source_2):
        operations += data_source_2.operations  # type: ignore
        # result of second pipeline will be second source in final operation
        pipeline_2_result = data_source_2.operations[-1].result # type: ignore
        final_operation_sources.append(pipeline_2_result)
    else:
        final_operation_sources.append(data_source_2)

    # Add last (or only) operation
    operations.append(options.op_class(final_operation_sources, options))

    return operations


def _is_data_pipeline(obj) -> bool:
    return hasattr(obj, 'data_sources') and hasattr(obj, 'operation_options')