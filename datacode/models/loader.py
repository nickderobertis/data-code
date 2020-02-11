from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional

import pandas as pd
from pd_utils.optimize.load import read_file

if TYPE_CHECKING:
    from datacode.models.source import DataSource


class DataLoader:

    def __init__(self, source: 'DataSource', read_file_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):
        if read_file_kwargs is None:
            read_file_kwargs = {}
        self.source = source
        self.optimize_size = optimize_size
        self.read_file_kwargs = read_file_kwargs

    def load(self) -> pd.DataFrame:
        df = self.read_file_into_df()
        self.rename_columns(df)
        if self.optimize_size:
            df = self.optimize_df_size(df)
        df = self.pre_transform_clean_up(df)
        df = self.apply_transforms(df)
        df = self.post_transform_clean_up(df)

        return df

    def read_file_into_df(self) -> pd.DataFrame:
        read_file_config = dict()
        if self.source.load_variables and self.source.columns:
            variable_keys = [var.key for var in self.source.load_variables]
            usecols = []
            for var_key in variable_keys:
                for col_key, col in self.source.columns.items():
                    if col.variable.key == var_key:
                        # Got column matching the desired variable
                        usecols.append(col_key)  # add the original column name in the dataset to usecols
            read_file_config['usecols'] = usecols

        read_file_config.update(self.read_file_kwargs)

        if self.source.location is None:
            return pd.DataFrame()
        return read_file(self.source.location, **read_file_config)

    def optimize_df_size(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO [#17]: implement df size optimization
        #
        # Needs to be after adding data types to variables. Then can use data types to optimize
        raise NotImplementedError('implement df size optimization')

    def rename_columns(self, df: pd.DataFrame):
        if not self.source.columns:
            return
        rename_dict = {}
        for orig_name, column in self.source.columns.items():
            variable = column.variable
            rename_dict[orig_name] = variable.name
        df.rename(columns=rename_dict, inplace=True)

    def apply_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.source.columns:
            return df

        # Create temporary source so that transform can have access to df and all columns with one object
        temp_source = deepcopy(self.source)
        temp_source.df = df
        temp_source.name = '_temp_source_for_transform'
        for column in self.source.columns.values():
            for transform in column.variable.applied_transforms:
                temp_source = transform.apply_transform_to_source(temp_source, column)
        return temp_source.df

    def pre_transform_clean_up(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform_clean_up(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
