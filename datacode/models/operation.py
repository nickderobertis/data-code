from typing import Sequence, Callable, Optional

from datacode.models.source import DataSource


class DataOperation:
    """
    Base class for a singlar data process that takes one or more DataSources as inputs and has one DataSource
    as the output.
    """

    def __init__(self, data_sources: Sequence[DataSource], options: 'OperationOptions',
                 output_name: Optional[str] = None):
        if output_name is None:
            names = [ds.name if ds.name is not None else 'unnamed' for ds in data_sources]
            output_name = ' & '.join(names) + ' Post-Operation'
        self.options = options
        self.data_sources = data_sources
        self.output_name = output_name
        self.result = None
        self._set_result()

    def execute(self):
        raise NotImplementedError('must implement execute in subclass of DataOperation')

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        raise NotImplementedError('must implement summary in subclass of DataOperation')

    def describe(self):
        raise NotImplementedError('must implement describe in subclass of DataOperation')

    def _set_result(self, **kwargs):
        self.result = DataSource(name=self.output_name)

    def __repr__(self):
        return f'<DataOperation(data_sources={self.data_sources}, result={self.result})>'


class OperationOptions:
    op_class = DataOperation
    """
    Base class for options passed to DataOperations
    """
    pass