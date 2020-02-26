from pandas.testing import assert_frame_equal

from datacode import DataSource, Column
from tests.pipeline.base import PipelineTest


class TestDataTransformationPipeline(PipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dtp = self.create_transformation_pipeline()
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df)

    def test_create_and_run_transformation_pipeline_from_transform(self):
        dtp = self.create_transformation_pipeline(func=self.source_transform)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_loaded_df_with_transform)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dtp = self.create_transformation_pipeline()

        ds = DataSource(pipeline=dtp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_func_df)

    def test_create_nested_generation_pipeline(self):
        dgp = self.create_generator_pipeline()

        dtp = self.create_transformation_pipeline(source=dgp)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_generated_transformed)

    def test_create_nested_merge_pipeline(self):
        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_merged_1_2_both_transformed)

    def test_original_variables_not_affected_by_transform(self):
        self.create_csv()
        a, b, c = self.create_variables()
        ac = Column(a, 'a')
        bc = Column(b, 'b')
        cc = Column(c, 'c')
        all_cols = [ac, bc, cc]
        source = self.create_source(df=None, columns=all_cols)
        dtp = self.create_transformation_pipeline(source=source)
        dtp.execute()

        assert not a.applied_transforms
        assert not b.applied_transforms
        assert not c.applied_transforms

    def test_transform_on_source_with_normal_and_transformed_of_same_variable(self):
        self.create_csv()
        a, b, c = self.create_variables(transform_data='cell', apply_transforms=False)
        ac = Column(a, 'a')
        bc = Column(b, 'b')
        cc = Column(c, 'c')
        all_cols = [ac, bc, cc]
        include_vars = [
            a,
            a.add_one_cell(),
            b,
            c
        ]
        source = self.create_source(df=None, columns=all_cols, load_variables=include_vars)
        dtp = self.create_transformation_pipeline(source=source)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df_with_a_and_a_transformed)
