import datetime
import json
import os
import warnings
from copy import deepcopy
from json.decoder import JSONDecodeError
from typing import Sequence, List, Callable, Optional, Union, Dict

from graphviz import Digraph
from mixins import ReprMixin

from datacode.graph.base import GraphObject, Graphable, GraphFunction
from datacode.graph.edge import Edge
from datacode.graph.node import Node
from datacode.graph.subgraph import Subgraph
import datacode.hooks as hooks
from datacode.logger import logger
from datacode.models.analysis import AnalysisResult
from datacode.models.dethash import DeterministicHashDictMixin
from datacode.models.last_modified import LinkedLastModifiedItem, most_recent_last_modified
from datacode.models.pipeline.operations.load import LoadOptions, LoadOperation
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.types import DataSourcesOrPipelines, DataSourceOrPipeline, ObjWithLastModified


class DataPipeline(LinkedLastModifiedItem, Graphable, DeterministicHashDictMixin, ReprMixin):
    """
    Base class for data pipelines. Should not be used directly.
    """
    repr_cols = ['name', 'data_sources', 'operation_options', 'difficulty']

    def __init__(self, data_sources: DataSourcesOrPipelines,
                 operation_options: Optional[Sequence[OperationOptions]],
                 name: Optional[str] = None, difficulty: float = 50):
        """

        :param data_sources:
        :param operation_options:
        :param name:
        """
        super().__init__()

        if operation_options is None:
            operation_options = []
        if data_sources is None:
            data_sources = []

        if not isinstance(data_sources, list):
            data_sources = list(data_sources)

        if not isinstance(operation_options, list):
            operation_options = list(operation_options)

        self.data_sources: List[DataSourceOrPipeline] = data_sources
        self.operation_options: List[OperationOptions] = operation_options
        self.name = name
        self.df = None
        self._operation_index = 0
        self.result = None
        self.difficulty = difficulty
        self._pre_execute_hash_dict: Dict[str, str] = {}

    def execute(self, output: bool = True):
        self._pre_execute_hash_dict = self.hash_dict()
        logger.debug(f'Executing pipeline {self}')
        hooks.on_begin_execute_pipeline(self)
        while True:
            try:
                self.next_operation()
            except LastOperationFinishedException:
                break

        self.result = self.operations[-1].result

        if output:
            self.output()

        hooks.on_end_execute_pipeline(self)
        logger.debug(f'Finished executing pipeline {self}')
        return self.result

    def next_operation(self):
        if self._operation_index == 0:
            self._set_df_from_first_operation()

        self._do_operation()

    def reset(self, forward: bool = False):
        self.df = None
        self._operation_index = 0
        self._set_operations()
        if forward:
            for item in self.forward_links:
                item.reset(forward=True)

    def _do_operation(self):
        try:
            operation = self.operations[self._operation_index]
            logger.info(f'Now running operation {self._operation_index + 1}: {operation}')
        except IndexError:
            raise LastOperationFinishedException

        operation.execute()

        # Set current df to result of merge
        if isinstance(operation.result, DataSource):
            # Need to check as may be analysis result, in which case df should not be changed
            self.df = operation.result.df

        self._operation_index += 1

    def _set_operations(self):
        self._operations = self._create_operations(self.data_sources, self.operation_options)

    def _create_operations(self, data_sources: DataSourcesOrPipelines, options_list: List[OperationOptions]):
        logger.debug(f'Creating operations for pipeline {self.name}')
        force_rerun = any([op.always_rerun for op in options_list])
        if not force_rerun and self.result_is_cached:
            # Already have result with the same exact config from a prior run. Just load it
            if options_list[-1].op_class.num_required_sources == 0:
                res = options_list[-1].get_operation(
                    self, options_list[-1], include_pipeline_in_result=True
                ).result
            elif options_list[-1].op_class.num_required_sources == 1:
                res = options_list[-1].get_operation(
                    self, [data_sources[0]], options_list[-1], include_pipeline_in_result=True
                ).result
            elif options_list[-1].op_class.num_required_sources == 2:
                res = options_list[-1].get_operation(
                    self, data_sources, options_list[-1], include_pipeline_in_result=True
                ).result
            else:
                raise ValueError('DataPipeline cannot handle operations with more than two sources')
            if isinstance(res, DataSource):
                load_options = LoadOptions(out_path=self.location, allow_modifying_result=self.allow_modifying_result,
                                           result_kwargs=options_list[-1].result_kwargs)
                load_operation = load_options.get_operation(self, [res], load_options)
                return [load_operation]
            warnings.warn(f'No loading from file implemented for result type {type(res)}, will always run pipeline')

        if len(options_list) == 1:
            result_opts = {'include_pipeline_in_result': True}
        else:
            result_opts = {}

        if options_list[0].op_class.num_required_sources == 0:
            operations = [options_list[0].get_operation(self, options_list[0], **result_opts)]
        elif options_list[0].op_class.num_required_sources == 1:
            operations = _get_operations_for_single(data_sources[0], options_list[0], self, **result_opts)
        elif options_list[0].op_class.num_required_sources == 2:
            operations = _get_operations_for_pair(
                data_sources[0], data_sources[1], options_list[0], self, **result_opts
            )
        else:
            raise ValueError('DataPipeline cannot handle operations with more than two sources')

        if len(options_list) == 1:
            logger.debug(f'Created single operation for pipeline {self.name}: {operations[0]}')
            return operations

        for i, options in enumerate(options_list[1:]):
            if i + 2 == len(options_list):
                # Include pipeline for last operation
                result_opts = {'include_pipeline_in_result': True}
            else:
                result_opts = {}

            if options.op_class.num_required_sources == 0:
                operations.append(options.get_operation(self, options, **result_opts))
            elif options.op_class.num_required_sources == 1:
                operations += _get_operations_for_single(operations[-1].result, options, self, **result_opts)
            elif options.op_class.num_required_sources == 2:
                operations += _get_operations_for_pair(
                    operations[-1].result, data_sources[i + 2], options, self, **result_opts
                )
            else:
                raise ValueError('DataPipeline cannot handle operations with more than two sources')

        logger.debug(f'Created operations for pipeline {self.name}: {operations}')
        return operations

    def _set_df_from_first_operation(self):
        logger.debug(f'Setting pipeline {self.name} df from first operation')
        # Need to check as may be generation pipeline which would not have a df to start from
        if hasattr(self.operations[0], 'data_sources') and self.operations[0].data_sources:
            self.df = self.operations[0].data_sources[0].df

    @property
    def operations(self) -> List[DataOperation]:
        try:
            return self._operations
        except AttributeError:
            self._set_operations()

        return self._operations

    # Following properties exist to recreate operations if data sources or merge options are overridden
    # by user

    @property
    def data_sources(self) -> List[DataSourceOrPipeline]:
        return self._data_sources

    @data_sources.setter
    def data_sources(self, data_sources: List[DataSourceOrPipeline]):
        self._data_sources = data_sources
        if data_sources is not None:
            for ds in data_sources:
                if ds is not None:
                    self._add_back_link(ds)
                    ds._add_forward_link(self)
        # only set operations if previously set. otherwise no need to worry about updating cached result
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

    def output(self):

        if self.result is None:
            return
        if isinstance(self.operations[-1], LoadOperation):
            # No reason to output if operation was load, there would be no change
            return
        if isinstance(self.result, AnalysisResult):
            if not self.operation_options[-1].can_output:
                return
            logger.debug(f'Outputting analysis result {self.result} from pipeline {self.name}')
            self.operation_options[-1].analysis_output_func(self.result, self.operation_options[-1].out_path)
            return
        if not isinstance(self.result, DataSource):
            raise NotImplementedError(f'have not implemented pipeline output for type {type(self.result)}')
        self.result.location = self.location
        if not self.operation_options[-1].can_output:
            return

        logger.debug(f'Outputting data source result {self.result} from pipeline {self.name}')
        # By default, save calculated variables, unless user explicitly passes to not save them
        # Essentially setting the opposite default versus working directly with the DataSource since
        # usually DataSource calculations are done on loading and it is assumed if the pipeline result
        # is being saved at all then it is likely an expensive calculation which the user doesn't
        # want to repeat on every load
        if 'save_calculated' not in self.result.data_outputter_kwargs:
            extra_kwargs = dict(save_calculated=True)
        else:
            extra_kwargs = {}

        self.result.output(**extra_kwargs)

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

    @property
    def location(self) -> Optional[str]:
        if self.result is not None and self.result.location is not None:
            return self.result.location

        if not self.operation_options[-1].can_output:
            return None

        return self.operation_options[-1].out_path

    @property
    def cache_json_location(self) -> Optional[str]:
        if self.location is None:
            return None

        return f'{self.location}.dcc.json'

    @property
    def result_is_cached(self) -> bool:
        loc = self.cache_json_location
        if loc is None:
            return False
        if not os.path.exists(loc):
            return False

        try:
            with open(loc, 'r') as f:
                cache = json.load(f)
        except JSONDecodeError:
            # Invalid json, treat as no match
            logger.warning(f'Got an invalid cache dict {loc}. Treating as if no cache.')
            return False

        return cache == self.hash_dict()

    @property
    def last_modified(self) -> Optional[datetime.datetime]:
        logger.debug(f'Determining last_modified in pipeline {self.name}')
        lm = None
        for obj in self.operations:
            lm = most_recent_last_modified(lm, obj.last_modified)
        return lm

    @property
    def allow_modifying_result(self) -> bool:
        return self.operation_options[-1].allow_modifying_result

    def touch(self):
        """
        Mark last_modified as now
        """
        for op in self.operations:
            # Don't mark last_modified on operations belonging to other pipelines
            if op.pipeline is self:
                op.last_modified = datetime.datetime.now()

    def copy(self):
        return deepcopy(self)

    def _graph_contents(self, include_attrs: Optional[Sequence[str]] = None,
                        func_dict: Optional[Dict[str, GraphFunction]] = None) -> List[GraphObject]:
        pn = self.primary_node(include_attrs, func_dict)
        elems = [pn]
        for source in self.data_sources:
            elems.extend(source._graph_contents(include_attrs, func_dict))
            edge = Edge(source.primary_node(include_attrs, func_dict), pn)
            elems.append(edge)
        return elems


class LastOperationFinishedException(Exception):
    pass


def _get_operations_for_single(data_source: DataSourceOrPipeline, options: OperationOptions,
                               current_pipeline: DataPipeline,
                               include_pipeline_in_result: bool = False) -> List[DataOperation]:
    """
     Creates a list of DataOperation/subclass objects from a single DataSource or DataPipeline object
    :param data_source:
    :param options: Options for the main operation
    :return:
    """
    operations: List[DataOperation] = []
    final_operation_sources: List[DataSource] = []
    # Add any pipeline operations first, as the results from the pipeline must be ready before we can use the results
    # for other data sources or pipeline results operations
    if _is_data_pipeline(data_source):
        operations += data_source.operations  # type: ignore
        pipeline_result = data_source.operations[-1].result  # type: ignore
        # result of first pipeline will be first source in final operation
        final_operation_sources.append(pipeline_result)
    else:
        final_operation_sources.append(data_source)  # type: ignore

    # Add last (or only) operation
    operations.append(
        options.get_operation(
            current_pipeline, final_operation_sources, options, include_pipeline_in_result=include_pipeline_in_result
        )
    )

    return operations


def _get_operations_for_pair(data_source_1: DataSourceOrPipeline, data_source_2: DataSourceOrPipeline,
                             options: OperationOptions, current_pipeline: DataPipeline,
                             include_pipeline_in_result: bool = False) -> List[DataOperation]:
    """
    Creates a list of DataOperation/subclass objects from a paring of two DataSource objects, a DataSource and a
    DataPipeline, or two DataPipeline objects.
    :param data_source_1: DataSource or DataMergePipeline
    :param data_source_2: DataSource or DataMergePipeline
    :param options: Options for the main operation
    :return: list of DataOperation/subclass objects
    """
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
    operations.append(
        options.get_operation(
            current_pipeline, final_operation_sources, options, include_pipeline_in_result=include_pipeline_in_result
        )
    )

    return operations


def _is_data_pipeline(obj) -> bool:
    return hasattr(obj, 'data_sources') and hasattr(obj, 'operation_options')