import os
from copy import deepcopy
from typing import TYPE_CHECKING, Optional, Dict, Any

import pandas as pd

if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.variables.variable import Variable


class DataOutputter:

    def __init__(self, source: 'DataSource', to_location_kwargs: Optional[Dict[str, Any]] = None,
                 safe: bool = True, preserve_original: bool = True, save_calculated: bool = False):
        if to_location_kwargs is None:
            to_location_kwargs = {}

        self.source = source
        self.to_location_kwargs = to_location_kwargs
        self.safe = safe
        self.preserve_original = preserve_original
        self.save_calculated = save_calculated

        self._validate()

    def _validate(self):
        if self.source.location is None:
            raise ValueError(f'must pass location to DataSource {self.source} to output it')
        if self.safe:
            self._check_safety()

    def _check_safety(self):
        if not self.source.columns:
            raise DataOutputNotSafeException(f'DataSource {self.source} has no columns, '
                                             f'cannot determine how to output safely. Pass safe=False to bypass')

        if self._output_exists and len(self.source.load_variables) < len(self.source.columns):
            raise DataOutputNotSafeException(f'DataSource {self.source} has {len(self.source.columns)} columns '
                                             f'but only {len(self.source.load_variables)} loaded variables, so '
                                             f'would be deleting existing data. Pass safe=False to bypass')

        if len(self.source.load_variables) > len(self.source.columns):
            raise DataOutputNotSafeException(f'DataSource {self.source} has {len(self.source.columns)} columns '
                                             f'but {len(self.source.load_variables)} loaded variables, so '
                                             f'some variables would not be outputted. Pass safe=False to bypass ')

    @property
    def _output_exists(self) -> bool:
        return os.path.exists(self.source.location)

    def output(self):
        if self.preserve_original:
            df = deepcopy(self.source.df)
        else:
            df = self.source.df
        self.rename_columns(df)
        self.keep_necessary_cols(df)
        self.output_to_location(df)

    def rename_columns(self, df: pd.DataFrame):
        if not self.source.columns:
            return

        rename_dict = {}
        for variable in self.source.load_variables:
            if variable.calculation is not None and not self.save_calculated:
                continue
            col = self.source.col_for(variable)
            rename_dict[variable.name] = col.load_key

        df.rename(columns=rename_dict, inplace=True)
        df.rename_axis(index=rename_dict, inplace=True)

    def keep_necessary_cols(self, df: pd.DataFrame):
        if not self.source.columns:
            return

        keep_cols = [col for col in df.columns if col in self.source.col_load_keys]

        num_cols = len(keep_cols) + len(df.index.names)
        if self.safe and num_cols < len(self.source.columns):
            existing_cols = keep_cols + df.index.names
            missing_cols = [col for col in self.source.col_load_keys if col not in existing_cols]
            raise DataOutputNotSafeException(f'After keeping necessary columns, only outputting '
                                             f'{num_cols} '
                                             f'columns when DataSource '
                                             f'describes {len(self.source.columns)} columns. '
                                             f'Missing columns {missing_cols}. '
                                             f'Pass safe=False to bypass.')

        drop_cols = [col for col in df.columns if col not in keep_cols]
        df.drop(drop_cols, axis=1, inplace=True)

    def output_to_location(self, df: pd.DataFrame):
        if not self.source.index_cols:
            # No index columns, index should be useless autoincrementing range, don't output it
            output_index = False
        else:
            output_index = True
        config_dict = deepcopy(self.to_location_kwargs)
        config_dict.update(dict(index=output_index))

        # TODO [#60]: implement output to location types other than CSV
        df.to_csv(self.source.location, **config_dict)



class DataOutputNotSafeException(Exception):
    pass