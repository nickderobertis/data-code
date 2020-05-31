import os
from typing import Optional, Tuple, Sequence, List

import pandas as pd
from numpy import nan

from datacode import AnalysisOptions, DataSource, DataAnalysisPipeline, DataGeneratorPipeline, GenerationOptions, \
    Variable, Column, MergeOptions, DataMergePipeline, SourceTransform, DataTransformationPipeline, TransformOptions, \
    ColumnIndex
from datacode.models.pipeline.combine import DataCombinationPipeline
from datacode.models.pipeline.operations.combine import CombineOptions
from datacode.models.types import DataSourceOrPipeline
from tests.test_source import SourceTest
from tests.utils import GENERATED_PATH


def analysis_from_source(ds: DataSource) -> float:
    running_sum = 0
    for var in ds.load_variables:
        if var.dtype.is_numeric:
            running_sum += ds.df[var.name].sum()
    return running_sum


EXPECT_GENERATED_DF = df = pd.DataFrame(
    [
        (1, 2, 'd'),
        (3, 4, 'e')
    ],
    columns=['a', 'b', 'C']
).convert_dtypes()


def ds_generator_func(columns: Sequence[Column]) -> DataSource:
    ds = DataSource(df=EXPECT_GENERATED_DF, columns=columns)
    return ds


def source_transform_func(ds: DataSource) -> DataSource:
    for variable in ds.load_variables:
        if variable.dtype.is_numeric:
            ds.df[variable.name] += 1
    return ds


class PipelineTest(SourceTest):
    merge_var = Variable('c', 'C', dtype='str')
    source_transform = SourceTransform('st', name_func=SourceTest.transform_name_func, data_func=source_transform_func)
    ds_one_analysis_result = 21
    ds_one_and_two_analysis_result = 191
    ds_one_transformed_analysis_result = 27
    ds_one_generated_analysis_result = 10
    csv_path2 = os.path.join(GENERATED_PATH, 'data2.csv')
    csv_path3 = os.path.join(GENERATED_PATH, 'data3.csv')
    csv_path_output = os.path.join(GENERATED_PATH, 'output.csv')

    test_df2 = pd.DataFrame(
        [
            (10, 20, 'd'),
            (50, 60, 'e'),
        ],
        columns=['e', 'f', 'c']
    )
    test_df3 = pd.DataFrame(
        [
            (100, 200, 'd'),
            (500, 600, 'e'),
        ],
        columns=['g', 'h', 'c']
    )
    expect_merged_1_2 = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20),
            (3, 4, 'd', 10, 20),
            (5, 6, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    ).convert_dtypes()
    expect_merged_1_2_c_index = expect_merged_1_2.set_index('C')
    expect_merged_1_transformed_2 = pd.DataFrame(
        [
            (2, 3, 'd', 10, 20),
            (4, 5, 'd', 10, 20),
            (6, 7, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    ).convert_dtypes()
    expect_merged_1_2_both_transformed = pd.DataFrame(
        [
            (2, 3, 'd', 11, 21),
            (4, 5, 'd', 11, 21),
            (6, 7, 'e', 51, 61),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    ).convert_dtypes()
    expect_merged_1_generated_2 = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20),
            (3, 4, 'e', 50, 60),
        ],
        columns=['a', 'b', 'C', 'E', 'F']
    ).convert_dtypes()
    expect_merged_1_2_3 = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20, 100, 200),
            (3, 4, 'd', 10, 20, 100, 200),
            (5, 6, 'e', 50, 60, 500, 600),
        ],
        columns=['A', 'B', 'C', 'E', 'F', 'G', 'H']
    ).convert_dtypes()
    expect_func_df = df = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['A', 'B', 'C']
    ).convert_dtypes()
    expect_func_df_with_a_and_a_transformed = pd.DataFrame(
        [
            (2, 3, 3, 'd'),
            (4, 5, 5, 'd'),
            (6, 7, 7, 'e')
        ],
        columns=['A', 'A_1', 'B', 'C']
    ).convert_dtypes()
    expect_loaded_df_with_transform = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['A_1', 'B_1', 'C_1']
    ).convert_dtypes()
    expect_generated_transformed = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'e')
        ],
        columns=['a', 'b', 'C']
    ).convert_dtypes()
    expect_combined_rows_1_2 = pd.DataFrame(
        [
            (1, 2, 'd', nan, nan),
            (3, 4, 'd', nan, nan),
            (5, 6, 'e', nan, nan),
            (nan, nan, 'd', 10, 20),
            (nan, nan, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F'],
    ).convert_dtypes()
    expect_combined_rows_1_2_c_index = expect_combined_rows_1_2.set_index('C')
    expect_combined_rows_1_2_3 = pd.DataFrame(
        [
            (1, 2, 'd', nan, nan, nan, nan),
            (3, 4, 'd', nan, nan, nan, nan),
            (5, 6, 'e', nan, nan, nan, nan),
            (nan, nan, 'd', 10, 20, nan, nan),
            (nan, nan, 'e', 50, 60, nan, nan),
            (nan, nan, 'd', nan, nan, 100, 200),
            (nan, nan, 'e', nan, nan, 500, 600),
        ],
        columns=['A', 'B', 'C', 'E', 'F', 'G', 'H']
    ).convert_dtypes()
    expect_combined_rows_1_2_entity_drop_c = pd.DataFrame(
        [
            (1, 2, 'd', nan, nan),
            (3, 4, 'd', nan, nan),
            (5, 6, 'e', nan, nan),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    ).convert_dtypes()
    expect_combined_rows_1_2_row_drop_c = pd.DataFrame(
        [
            (1, 2, 'd', nan, nan),
            (5, 6, 'e', nan, nan),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    ).convert_dtypes()





    def create_csv_for_2(self, df: Optional[pd.DataFrame] = None, **to_csv_kwargs):
        if df is None:
            df = self.test_df2
        df.to_csv(self.csv_path2, index=False, **to_csv_kwargs)

    def create_csv_for_3(self, df: Optional[pd.DataFrame] = None, **to_csv_kwargs):
        if df is None:
            df = self.test_df3
        df.to_csv(self.csv_path3, index=False, **to_csv_kwargs)

    def create_variables_for_2(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        transform_dict = self.get_transform_dict(transform_data=transform_data, apply_transforms=apply_transforms)

        e = Variable('e', 'E', dtype='int', **transform_dict)
        f = Variable('f', 'F', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return e, f, c

    def create_variables_for_3(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        transform_dict = self.get_transform_dict(transform_data=transform_data, apply_transforms=apply_transforms)

        g = Variable('g', 'G', dtype='int', **transform_dict)
        h = Variable('h', 'H', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return g, h, c

    def create_variables_for_generated(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        transform_dict = self.get_transform_dict(transform_data=transform_data, apply_transforms=apply_transforms)

        a = Variable('a', 'a', dtype='int', **transform_dict)
        b = Variable('b', 'b', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return a, b, c

    def create_columns_for_2(self, transform_data: str = '', apply_transforms: bool = True):
        e, f, c = self.create_variables_for_2(transform_data=transform_data, apply_transforms=apply_transforms)
        ec = Column(e, 'e')
        fc = Column(f, 'f')
        cc = Column(c, 'c')
        return [
            ec,
            fc,
            cc
        ]

    def create_columns_for_3(self, transform_data: str = '', apply_transforms: bool = True):
        g, h, c = self.create_variables_for_3(transform_data=transform_data, apply_transforms=apply_transforms)
        gc = Column(g, 'g')
        hc = Column(h, 'h')
        cc = Column(c, 'c')
        return [
            gc,
            hc,
            cc
        ]

    def create_columns_for_generated(self, transform_data: str = '', apply_transforms: bool = True):
        a, b, c = self.create_variables_for_generated(transform_data=transform_data, apply_transforms=apply_transforms)
        ac = Column(a, 'a')
        bc = Column(b, 'b')
        cc = Column(c, 'C')
        return [
            ac,
            bc,
            cc
        ]

    def create_variables_and_c_colindex_for_2(self, transform_data: str = '', apply_transforms: bool = True
                                        ) -> Tuple[List[Variable], ColumnIndex]:
        e, f, c = self.create_variables_for_2(transform_data=transform_data, apply_transforms=apply_transforms)
        c_index = self.create_c_index()

        c_col_index = ColumnIndex(c_index, [c])

        return [e, f, c], c_col_index

    def create_indexed_columns_for_2(self, transform_data: str = '', apply_transforms: bool = True) -> List[Column]:
        (e, f, c), c_col_index = self.create_variables_and_c_colindex_for_2(
            transform_data=transform_data, apply_transforms=apply_transforms
        )
        ec = Column(e, 'e', indices=[c_col_index])
        fc = Column(f, 'f', indices=[c_col_index])
        cc = Column(c, 'c')
        return [
            ec,
            fc,
            cc
        ]

    def create_variables_and_c_colindex_for_3(self, transform_data: str = '', apply_transforms: bool = True
                                        ) -> Tuple[List[Variable], ColumnIndex]:
        g, h, c = self.create_variables_for_3(transform_data=transform_data, apply_transforms=apply_transforms)
        c_index = self.create_c_index()

        c_col_index = ColumnIndex(c_index, [c])

        return [g, h, c], c_col_index

    def create_indexed_columns_for_3(self, transform_data: str = '', apply_transforms: bool = True) -> List[Column]:
        (g, h, c), c_col_index = self.create_variables_and_c_colindex_for_3(
            transform_data=transform_data, apply_transforms=apply_transforms
        )
        gc = Column(g, 'g', indices=[c_col_index])
        hc = Column(h, 'h', indices=[c_col_index])
        cc = Column(c, 'c')
        return [
            gc,
            hc,
            cc
        ]

    def create_merge_pipeline(self, include_indices: Sequence[int] = (0, 1),
                              data_sources: Optional[Sequence[DataSource]] = None,
                              merge_options_list: Optional[Sequence[MergeOptions]] = None,
                              indexed: bool = False) -> DataMergePipeline:
        if indexed:
            col_func_1 = self.create_indexed_columns
            col_func_2 = self.create_indexed_columns_for_2
            col_func_3 = self.create_indexed_columns_for_3
        else:
            col_func_1 = self.create_columns
            col_func_2 = self.create_columns_for_2
            col_func_3 = self.create_columns_for_3


        if data_sources is None:
            self.create_csv()
            ds1_cols = col_func_1()
            ds1 = self.create_source(df=None, columns=ds1_cols, name='one')
            self.create_csv_for_2()
            ds2_cols = col_func_2()
            ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
            self.create_csv_for_3()
            ds3_cols = col_func_3()
            ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')


            data_sources = [ds1, ds2, ds3]
            selected_data_sources = []
            for i, ds in enumerate(data_sources):
                if i in include_indices:
                    selected_data_sources.append(ds)
        else:
            selected_data_sources = data_sources

        if merge_options_list is None:
            mo = MergeOptions([self.merge_var.name])
            merge_options_list = [mo for _ in range(len(selected_data_sources) - 1)]

        merge_options_list[-1].out_path = self.csv_path_output

        dp = DataMergePipeline(selected_data_sources, merge_options_list)
        return dp


    def create_analysis_pipeline(self, source: Optional[DataSourceOrPipeline] = None,
                                 options: Optional[AnalysisOptions] = None):
        if source is None:
            self.create_csv()
            ds1_cols = self.create_columns()
            source = self.create_source(df=None, columns=ds1_cols, name='one')

        if options is None:
            options = AnalysisOptions(analysis_from_source)

        dap = DataAnalysisPipeline(source, options)
        return dap

    def create_generator_pipeline(self) -> DataGeneratorPipeline:
        gen_cols = self.create_columns_for_generated()
        go = GenerationOptions(ds_generator_func, out_path=self.csv_path_output, columns=gen_cols)
        dgp = DataGeneratorPipeline(go)
        return dgp

    def create_transformation_pipeline(self, source: Optional[DataSourceOrPipeline] = None,
                                       **pipeline_kwargs) -> DataTransformationPipeline:
        config_dict = dict(
            func=source_transform_func,
            out_path=self.csv_path_output
        )
        config_dict.update(pipeline_kwargs)
        if source is None:
            self.create_csv()
            all_cols = self.create_columns()
            source = self.create_source(df=None, columns=all_cols)

        to = TransformOptions(**config_dict)

        dtp = DataTransformationPipeline(source, to)
        return dtp

    def create_combine_pipeline(self, include_indices: Sequence[int] = (0, 1),
                                data_sources: Optional[Sequence[DataSource]] = None,
                                combine_options_list: Optional[Sequence[CombineOptions]] = None,
                                indexed: bool = False):
        if indexed:
            col_func_1 = self.create_indexed_columns
            col_func_2 = self.create_indexed_columns_for_2
            col_func_3 = self.create_indexed_columns_for_3
        else:
            col_func_1 = self.create_columns
            col_func_2 = self.create_columns_for_2
            col_func_3 = self.create_columns_for_3


        if data_sources is None:
            self.create_csv()
            ds1_cols = col_func_1()
            ds1 = self.create_source(df=None, columns=ds1_cols, name='one')
            self.create_csv_for_2()
            ds2_cols = col_func_2()
            ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
            self.create_csv_for_3()
            ds3_cols = col_func_3()
            ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')


            data_sources = [ds1, ds2, ds3]
            selected_data_sources = []
            for i, ds in enumerate(data_sources):
                if i in include_indices:
                    selected_data_sources.append(ds)
        else:
            selected_data_sources = data_sources

        if combine_options_list is None:
            mo = CombineOptions()
            combine_options_list = [mo for _ in range(len(selected_data_sources) - 1)]

        combine_options_list[-1].out_path = self.csv_path_output

        dp = DataCombinationPipeline(selected_data_sources, combine_options_list)
        return dp