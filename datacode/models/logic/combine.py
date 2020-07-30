from typing import Sequence, Any, Optional, List, Tuple, Dict, Union, Set

from datacode.logger import logger
from datacode.models.dtypes.base import DataType
from typing_extensions import Protocol
import pandas as pd
from datacode.models.column.column import Column

from datacode.models.variables import Variable
from datacode.models.source import DataSource, NoColumnForVariableException


def combine_sources(data_sources: Sequence[DataSource], rows: bool = True,
                    row_duplicate_vars: Optional[Sequence[Variable]] = None,
                    entity_duplicate_vars: Optional[Sequence[Variable]] = None) -> DataSource:
    if rows:
        return _combine_rows(
            data_sources,
            row_duplicate_vars=row_duplicate_vars,
            entity_duplicate_vars=entity_duplicate_vars
        )

    # Columns
    return _combine_columns(data_sources)


def _combine_columns(data_sources: Sequence[DataSource]) -> DataSource:
    if data_sources[0].index_cols != data_sources[1].index_cols:
        if _column_lists_match_excluding_load_keys(data_sources[0].index_cols, data_sources[1].index_cols):
            _warn_about_mismatching_load_keys(data_sources[0].index_cols, data_sources[1].index_cols)
        else:
            mismatching_message = _mismatching_attributes_of_columns_message(
                data_sources[0].index_cols, data_sources[1].index_cols
            )
            raise ValueError(f'can only combine columns of data sources with overlapping indices. '
                             f'{mismatching_message}')

    new_vars, new_cols = _combine_variables_and_columns(data_sources, allow_overlap=False)
    # new_df = pd.concat([ds.df for ds in data_sources], axis=1)
    new_df = data_sources[0].df.join(data_sources[1].df, how='outer')
    new_source = DataSource(df=new_df, columns=new_cols, load_variables=new_vars)

    return new_source


def _combine_rows(
    data_sources: Sequence[DataSource],
    row_duplicate_vars: Optional[Sequence[Variable]] = None,
    entity_duplicate_vars: Optional[Sequence[Variable]] = None
) -> DataSource:
    if row_duplicate_vars and entity_duplicate_vars:
        raise ValueError('pass at most one of row_duplicate_vars and entity_duplicate_vars')

    reset_index = False
    if not data_sources[0].index_cols and not data_sources[1].index_cols:
        reset_index = True

    new_vars, new_cols = _combine_variables_and_columns(data_sources, allow_overlap=True)

    if not row_duplicate_vars and not entity_duplicate_vars:
        # Simple append
        new_df = pd.concat([ds.df for ds in data_sources])
    elif row_duplicate_vars:
        # Drop duplicates by rows matching row_duplicate_vars, take first source
        new_df = _append_handling_categorical_index(data_sources[0].df, data_sources[1].df)
        _drop_duplicates_by_vars(new_df, row_duplicate_vars)
    else: # entity_duplicate_vars
        # Select entities from the second data source which are not in the first data source, and only
        # add those
        entity_col_names = [var.name for var in entity_duplicate_vars]
        existing_entities = _get_entities(data_sources[0].df, entity_col_names)
        potential_entities = _get_entities(data_sources[1].df, entity_col_names)
        new_entities = list(potential_entities - existing_entities)
        new_rows = _select_df_by_tuples(data_sources[1].df, new_entities, entity_col_names)
        new_df = _append_handling_categorical_index(data_sources[0].df, new_rows)

    if reset_index:
        new_df.reset_index(drop=True, inplace=True)

    # Set output data types
    for col in new_cols:
        col_from_all_sources = []
        for ds in data_sources:
            try:
                col = ds.col_for(var_key=col.variable.unique_key, is_unique_key=True)
            except NoColumnForVariableException:
                # column was not in this source
                continue
            col_from_all_sources.append(col)
        dtypes: Set[DataType] = set([v.dtype for v in col_from_all_sources])
        if len(dtypes) > 1:
            raise NotImplementedError(f'got multiple dtypes {dtypes} for {col} during combine, '
                                      f'need to provide a way for the user to specify output type')
        if len(dtypes) == 0:
            raise ValueError(f'could not extract a data type for {col}')

        dtype = list(dtypes)[0]
        if col.variable.name in new_df.index.names:
            # Variable is in index. Need to handle differently
            if len(new_df.index.names) == 1:
                # Variable is only index
                new_df.index = new_df.index.astype(dtype.index_arg)
                col.series = pd.Series(new_df.index)
            else:
                # Multi-index, need to replace only this level
                idx_pos = new_df.index.names.index(col.variable.name)
                converted = new_df.index.levels[idx_pos].astype(dtype.index_arg)
                new_df.index = new_df.index.set_levels(converted, level=idx_pos)
                col.series = pd.Series(converted)
        else:
            # Variable is in columns, not index
            new_df[col.variable.name] = new_df[col.variable.name].astype(dtype.pd_class())
            col.series = new_df[col.variable.name]

    new_source = DataSource(df=new_df, columns=new_cols, load_variables=new_vars)

    return new_source


def _drop_duplicates_by_vars(df: pd.DataFrame, variables: Sequence[Variable]):
    """
    Drops duplicates on a subset of variables regardless of whether they are in the index

    Notes:
        inplace

    :param df:
    :param variables:
    :return:
    """
    orig_index_cols = df.index.names
    remove_index_name = False
    if orig_index_cols == [None]:
        orig_index_cols = ['index']
        remove_index_name = True

    df.reset_index(inplace=True)
    df.drop_duplicates(subset=[var.name for var in variables], inplace=True)
    df.set_index(orig_index_cols, inplace=True)

    if remove_index_name:
        df.index.name = None


def _append_handling_categorical_index(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Resets index, concatenates rows, then sets index back. Only necessary
    because of this pandas issue: https://github.com/pandas-dev/pandas/issues/17629

    Use a regular concat over axis=0 once this issue is resolved.

    :param df:
    :param df2:
    :return:
    """
    if df1.index.names != df2.index.names:
        raise ValueError('must append with identical index')

    orig_index_cols = df1.index.names
    remove_index_name = False
    if orig_index_cols == [None]:
        orig_index_cols = ['index']
        remove_index_name = True

    combined = pd.concat([df.reset_index() for df in [df1, df2]])

    combined.set_index(orig_index_cols, inplace=True)
    if remove_index_name:
        combined.index.name = None
    return combined


def _get_entities(df: pd.DataFrame, entity_col_names: Sequence[str]) -> Set[Sequence[Any]]:
    return set([tuple(r) for r in df.reset_index()[entity_col_names].to_numpy().tolist()])


def _select_df_by_tuples(df: pd.DataFrame, tuples: List[Sequence[Any]],
                         col_names: Sequence[Union[str, int, float]]) -> pd.DataFrame:
    orig_index_cols = df.index.names
    remove_index_name = False
    if orig_index_cols == [None]:
        orig_index_cols = ['index']
        remove_index_name = True

    # Set index to what we are querying by
    df.reset_index(inplace=True)
    df.set_index(col_names, inplace=True)

    # Selection is different when only a single column
    if len(col_names) == 1:
        to_select = [tup[0] for tup in tuples]
    else:
        to_select = tuples

    selected = df.loc[to_select]

    # Set index back to original
    for dataframe in [df, selected]:
        dataframe.reset_index(inplace=True)
        dataframe.set_index(orig_index_cols, inplace=True)
        if remove_index_name:
            dataframe.index.name = None

    return selected


def _combine_variables_and_columns(
    data_sources: Sequence[DataSource],
    allow_overlap: bool = False
) -> Tuple[List[Variable], List[Column]]:
    all_variables = []
    all_columns = []

    for ds in data_sources:
        for var in ds.load_variables:
            if var in all_variables:
                if allow_overlap:
                    continue
                else:
                    if ds.col_for(var) in ds.index_cols:
                        continue
                    raise ValueError(f'variable {var} exists in multiple data sources')
            all_variables.append(var)
            all_columns.append(ds.col_for(var))
    return all_variables, all_columns


def _column_lists_match_excluding_load_keys(cols1: Sequence[Column], cols2: Sequence[Column]) -> bool:
    for col1, col2 in zip(cols1, cols2):
        if not all([
            col1.variable == col2.variable,
            col1.indices == col2.indices,
            col1.applied_transform_keys == col2.applied_transform_keys,
            col1.dtype == col2.dtype
        ]):
            return False

    return True


def _warn_about_mismatching_load_keys(cols1: Sequence[Column], cols2: Sequence[Column]):
    for col1, col2 in zip(cols1, cols2):
        if col1.variable != col2.variable:
            raise ValueError('warn about mismatching must be called only on columns which match other than load_key')
        if col1.load_key != col2.load_key:
            logger.warning(f'Got both {col1.load_key} and {col2.load_key} as load_key for '
                          f'{col1.variable}, will use {col1.load_key}')


def _mismatching_attributes_of_columns_message(cols1: Sequence[Column], cols2: Sequence[Column]) -> Optional[str]:
    messages = []
    for col1, col2 in zip(cols1, cols2):
        for attr in col1.equal_attrs:
            val1 = getattr(col1, attr)
            val2 = getattr(col2, attr)
            if val1 != val2:
                messages.append(f'Column {col1.load_key} has {val1} for {attr} while {col2.load_key} has {val2}')
    if messages:
        return ', '.join(messages)


class CombineFunction(Protocol):

    def __call__(self, data_sources: Sequence[DataSource], **kwargs: Any) -> DataSource: ...