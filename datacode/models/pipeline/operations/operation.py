import datetime
from copy import deepcopy
from typing import Sequence, Callable, Optional, Type, TYPE_CHECKING, Dict, Any

from mixins.repr import ReprMixin

if TYPE_CHECKING:
    from datacode.models.analysis import AnalysisResult

from datacode.models.source import DataSource


class DataOperation:
    """
    Base class for a singlar data process that takes one or more DataSources as inputs and has one DataSource
    as the output.
    """
    num_required_sources: int = 1

    def __init__(self, data_sources: Sequence[DataSource], options: 'OperationOptions',
                 output_name: Optional[str] = None, **result_kwargs):
        if output_name is None:
            names = [ds.name if ds.name is not None else 'unnamed' for ds in data_sources]
            output_name = ' & '.join(names) + ' Post-Operation'
        self.options = options
        self.data_sources = data_sources
        self.output_name = output_name
        self.result = None
        self._set_result(**result_kwargs)

    def execute(self):
        raise NotImplementedError('must implement execute in subclass of DataOperation')

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        raise NotImplementedError('must implement summary in subclass of DataOperation')

    def describe(self):
        raise NotImplementedError('must implement describe in subclass of DataOperation')

    def _set_result(self, **kwargs):
        self.result = self.options.result_class(name=self.output_name, location=self.options.out_path, **kwargs)

    def __repr__(self):
        return f'<DataOperation(data_sources={self.data_sources}, result={self.result})>'


class OperationOptions(ReprMixin):
    """
    Base class for options passed to DataOperations
    """
    op_class: Type[DataOperation] = DataOperation
    result_class: Type = DataSource
    result_kwargs: Optional[Dict[str, Any]] = None
    out_path: Optional[str] = None
    last_modified: Optional[datetime.datetime] = None
    allow_modifying_result: bool = True
    analysis_output_func: Optional[Callable[['AnalysisResult', str], None]] = None
    repr_cols = [
        'out_path',
        'last_modified',
        'allow_modifying_result',
        'analysis_output_func'
    ]

    def copy(self):
        return deepcopy(self)

    @property
    def can_output(self) -> bool:
        return self.out_path is not None

    def get_operation(self, *args) -> DataOperation:
        kwargs = {}
        if self.result_kwargs is not None:
            kwargs = self.result_kwargs
        return self.op_class(*args, **kwargs)
