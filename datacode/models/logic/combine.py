from typing import Sequence, Any, Optional, List, Tuple, Dict, Union, Set

from typing_extensions import Protocol
import pandas as pd
from datacode.models.column.column import Column

from datacode.models.variables import Variable
from datacode.models.source import DataSource


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
        raise ValueError('can only combine columns of data sources with overlapping indices')

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
        new_df = pd.concat([ds.df for ds in data_sources])
        new_df.drop_duplicates(subset=[var.name for var in row_duplicate_vars], inplace=True)
    else: # entity_duplicate_vars
        # Select entities from the second data source which are not in the first data source, and only
        # add those
        entity_col_names = [var.name for var in entity_duplicate_vars]
        existing_entities = _get_entities(data_sources[0].df, entity_col_names)
        potential_entities = _get_entities(data_sources[0].df, entity_col_names)
        new_entities = list(potential_entities - existing_entities)
        new_rows = _select_df_by_tuples(data_sources[1].df, new_entities, entity_col_names)
        new_df = pd.concat([data_sources[0].df, new_rows])

    if reset_index:
        new_df.reset_index(drop=True, inplace=True)

    new_source = DataSource(df=new_df, columns=new_cols, load_variables=new_vars)


    return new_source


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


class CombineFunction(Protocol):

    def __call__(self, data_sources: Sequence[DataSource], **kwargs: Any) -> DataSource: ...