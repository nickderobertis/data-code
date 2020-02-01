import os
import datetime

import pandas as pd
from pandas.util.testing import assert_frame_equal
from pandas import Timestamp
from numpy import nan
import numpy

import datacode

# TODO [#5]: remove mock test once others are fixed
def test_mock():
    pass


class DataFrameTest:
    
    ticker_df = pd.DataFrame(data = [
        ('a', Timestamp('2000-01-01 00:00:00'), 'ADM'),
        ('a', Timestamp('2000-01-02 00:00:00'), 'ADM'),
        ('a', Timestamp('2000-01-03 00:00:00'), 'ADM'),
        ('a', Timestamp('2000-01-04 00:00:00'), 'ADM'),
        ('b', Timestamp('2000-01-01 00:00:00'), 'ADM'),
        ('b', Timestamp('2000-01-02 00:00:00'), 'ADM'),
        ('b', Timestamp('2000-01-03 00:00:00'), 'ADM'),
        ('b', Timestamp('2000-01-04 00:00:00'), 'ADM'),
        ('a', Timestamp('2008-01-01 00:00:00'), 'AAN'),
        ('a', Timestamp('2009-01-02 00:00:00'), 'AAN'),
        ('a', Timestamp('2010-01-03 00:00:00'), 'AAN'),
        ('a', Timestamp('2011-01-04 00:00:00'), 'AAN'),
        ], columns = ['byvar', 'Date', 'TICKER'])
    
    permno_df_with_nan = pd.DataFrame(data = [
        ('a', Timestamp('2000-01-01 00:00:00'), 10516.0),
        ('a', Timestamp('2000-01-02 00:00:00'), 10516.0),
        ('a', Timestamp('2000-01-03 00:00:00'), 10516.0),
        ('a', Timestamp('2000-01-04 00:00:00'), 10516.0),
        ('b', Timestamp('2000-01-01 00:00:00'), 10516.0),
        ('b', Timestamp('2000-01-02 00:00:00'), 10516.0),
        ('b', Timestamp('2000-01-03 00:00:00'), 10516.0),
        ('b', Timestamp('2000-01-04 00:00:00'), 10516.0),
        ('a', Timestamp('2008-01-01 00:00:00'), nan),
        ('a', Timestamp('2009-01-02 00:00:00'), nan),
        ('a', Timestamp('2010-01-03 00:00:00'), 78049.0),
        ('a', Timestamp('2011-01-04 00:00:00'), 10517.0),
        ], columns = ['byvar', 'Date', 'PERMNO'])
    
    df = pd.DataFrame([
                                (10516, 'a', '1/1/2000', 1.01),
                                (10516, 'a', '1/2/2000', 1.02),
                                (10516, 'a', '1/3/2000', 1.03),
                                (10516, 'a', '1/4/2000', 1.04),
                                (10516, 'b', '1/1/2000', 1.05),
                                (10516, 'b', '1/2/2000', 1.06),
                                (10516, 'b', '1/3/2000', 1.07),
                                (10516, 'b', '1/4/2000', 1.08),
                                (10517, 'a', '1/1/2000', 1.09),
                                (10517, 'a', '1/2/2000', 1.10),
                                (10517, 'a', '1/3/2000', 1.11),
                                (10517, 'a', '1/4/2000', 1.12),
                               ], columns = ['PERMNO','byvar','Date', 'RET'])   
    
    df_ticker_extra_cols_year_month = pd.DataFrame(data = [
        ('a', Timestamp('2000-01-01 00:00:00'), 'ADM', 1.01, 2000, 1),
        ('a', Timestamp('2000-01-02 00:00:00'), 'ADM', 1.02, 2000, 1),
        ('a', Timestamp('2000-01-03 00:00:00'), 'ADM', 1.03, 2000, 1),
        ('a', Timestamp('2000-01-04 00:00:00'), 'ADM', 1.04, 2000, 1),
        ('b', Timestamp('2000-01-01 00:00:00'), 'ADM', 1.05, 2000, 1),
        ('b', Timestamp('2000-01-02 00:00:00'), 'ADM', 1.06, 2000, 1),
        ('b', Timestamp('2000-01-03 00:00:00'), 'ADM', 1.07, 2000, 1),
        ('b', Timestamp('2000-01-04 00:00:00'), 'ADM', 1.08, 2000, 1),
        ('a', Timestamp('2008-01-01 00:00:00'), 'AAN', 1.09, 2008, 1),
        ('a', Timestamp('2009-01-02 00:00:00'), 'AAN', 1.1, 2009, 1),
        ('a', Timestamp('2010-01-03 00:00:00'), 'AAN', 1.11, 2010, 1),
        ('a', Timestamp('2011-01-04 00:00:00'), 'AAN', 1.12, 2011, 1),
        ], columns = ['byvar', 'Date', 'TICKER', 'other', 'Year', 'Month'])
    
    df_datetime = df.copy()
    df_datetime['Date'] = pd.to_datetime(df_datetime['Date'])
    
    df_gvkey_str = pd.DataFrame([
            ('001076','3/1/1995'),
            ('001076','4/1/1995'),
            ('001722','1/1/2012'),
            ('001722','7/1/2012'),
            ('001722', nan),
            (nan ,'1/1/2012')
            ], columns=['GVKEY','Date'])

    df_gvkey_str['Date'] = pd.to_datetime(df_gvkey_str['Date'])
    df_gvkey_num = df_gvkey_str.copy()
    df_gvkey_num['GVKEY'] = df_gvkey_num['GVKEY'].astype('float64')

# TODO [#6]: decouple get CRSP from data paths before getting these tests working
    
# class TestMergeDSENames(DataFrameTest):
#
#     def test_on_ticker_get_permno(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('a', Timestamp('2000-01-01 00:00:00'), 'ADM', 10516.0),
#             ('a', Timestamp('2000-01-02 00:00:00'), 'ADM', 10516.0),
#             ('a', Timestamp('2000-01-03 00:00:00'), 'ADM', 10516.0),
#             ('a', Timestamp('2000-01-04 00:00:00'), 'ADM', 10516.0),
#             ('b', Timestamp('2000-01-01 00:00:00'), 'ADM', 10516.0),
#             ('b', Timestamp('2000-01-02 00:00:00'), 'ADM', 10516.0),
#             ('b', Timestamp('2000-01-03 00:00:00'), 'ADM', 10516.0),
#             ('b', Timestamp('2000-01-04 00:00:00'), 'ADM', 10516.0),
#             ('a', Timestamp('2008-01-01 00:00:00'), 'AAN', nan),
#             ('a', Timestamp('2009-01-02 00:00:00'), 'AAN', nan),
#             ('a', Timestamp('2010-01-03 00:00:00'), 'AAN', 78049.0),
#             ('a', Timestamp('2011-01-04 00:00:00'), 'AAN', 10517.0),
#             ], columns = ['byvar', 'Date', 'TICKER', 'PERMNO'])
#
#         dse = datacode.merge_dsenames(self.ticker_df, other_byvars='byvar') #default is on ticker get permno
#
#         assert_frame_equal(expect_df, dse)
#
#     def test_on_ticker_get_permno_extra_cols(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('a', Timestamp('2000-01-01 00:00:00'), 'ADM', 1.01, 2000, 1, 10516.0),
#             ('a', Timestamp('2000-01-02 00:00:00'), 'ADM', 1.02, 2000, 1, 10516.0),
#             ('a', Timestamp('2000-01-03 00:00:00'), 'ADM', 1.03, 2000, 1, 10516.0),
#             ('a', Timestamp('2000-01-04 00:00:00'), 'ADM', 1.04, 2000, 1, 10516.0),
#             ('b', Timestamp('2000-01-01 00:00:00'), 'ADM', 1.05, 2000, 1, 10516.0),
#             ('b', Timestamp('2000-01-02 00:00:00'), 'ADM', 1.06, 2000, 1, 10516.0),
#             ('b', Timestamp('2000-01-03 00:00:00'), 'ADM', 1.07, 2000, 1, 10516.0),
#             ('b', Timestamp('2000-01-04 00:00:00'), 'ADM', 1.08, 2000, 1, 10516.0),
#             ('a', Timestamp('2008-01-01 00:00:00'), 'AAN', 1.09, 2008, 1, nan),
#             ('a', Timestamp('2009-01-02 00:00:00'), 'AAN', 1.1, 2009, 1, nan),
#             ('a', Timestamp('2010-01-03 00:00:00'), 'AAN', 1.11, 2010, 1, 78049.0),
#             ('a', Timestamp('2011-01-04 00:00:00'), 'AAN', 1.12, 2011, 1, 10517.0),
#             ], columns = ['byvar', 'Date', 'TICKER', 'other', 'Year', 'Month', 'PERMNO'])
#
#         dse = datacode.merge_dsenames(self.df_ticker_extra_cols_year_month, other_byvars='byvar')
#
#         assert_frame_equal(expect_df, dse)

# TODO [#7]: decouple get CRSP from data paths before getting get gvkey or permno tests working
# class TestGetGvkeyOrPermno(DataFrameTest):
#
#     def test_get_gvkey_with_nan(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('a', Timestamp('2000-01-01 00:00:00'), 10516.0, 1722),
#             ('a', Timestamp('2000-01-02 00:00:00'), 10516.0, 1722),
#             ('a', Timestamp('2000-01-03 00:00:00'), 10516.0, 1722),
#             ('a', Timestamp('2000-01-04 00:00:00'), 10516.0, 1722),
#             ('b', Timestamp('2000-01-01 00:00:00'), 10516.0, 1722),
#             ('b', Timestamp('2000-01-02 00:00:00'), 10516.0, 1722),
#             ('b', Timestamp('2000-01-03 00:00:00'), 10516.0, 1722),
#             ('b', Timestamp('2000-01-04 00:00:00'), 10516.0, 1722),
#             ('a', Timestamp('2008-01-01 00:00:00'), nan, nan),
#             ('a', Timestamp('2009-01-02 00:00:00'), nan, nan),
#             ('a', Timestamp('2010-01-03 00:00:00'), 78049.0, 1076),
#             ('a', Timestamp('2011-01-04 00:00:00'), 10517.0, 1076),
#             ], columns = ['byvar', 'Date', 'PERMNO', 'GVKEY'])
#
#         ggop = datacode.get_gvkey_or_permno(self.permno_df_with_nan, datevar='Date',
#                                              other_byvars='byvar') #default is on permno get gvkey
#
#         assert_frame_equal(expect_df, ggop)
        

# TODO [#8]: decouple get FF from data paths before getting these tests working

# class TestGetAbret(DataFrameTest):
#
#     def create_indf(self):
#         indf = self.df_datetime.copy()
#         indf['Date'] = indf['Date'] + datetime.timedelta(days=1) #push forward to get enough obs on trading days
#         return indf
#
#     def test_multiple_byvars_daily(self):
#         indf = self.create_indf()
#
#         expect_df = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.01, nan),
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.02, 1.020482537872975),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.03, 1.0327593010764478),
#             (10516, 'a', Timestamp('2000-01-05 00:00:00'), 1.04, 1.0400611667726307),
#             (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.05, nan),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.06, 1.060482537872975),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.07, 1.072759301076448),
#             (10516, 'b', Timestamp('2000-01-05 00:00:00'), 1.08, 1.0800611667726308),
#             (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.09, nan),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.1, 1.100482537872975),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.11, 1.1127593010764478),
#             (10517, 'a', Timestamp('2000-01-05 00:00:00'), 1.12, 1.1200611667726308),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET', 'ABRET'])
#
#         ga = datacode.get_abret(indf, ['PERMNO','byvar'], freq='d', abret_fac=1)
#
#         assert_frame_equal(expect_df, ga)
#
#     def test_multiple_byvars_daily_includecoef_includefac(self):
#         indf = self.create_indf()
#
#         expect_df = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.01, nan, nan, 1.031101001907351,
#              0.06796308070068235, nan),
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.02, -0.0071, 0.00021, 1.031101001907351,
#              0.06796308070068235, 1.020482537872975),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.03, -0.0406, 0.00021, 1.031101001907351,
#              0.06796308070068235, 1.0327593010764478),
#             (10516, 'a', Timestamp('2000-01-05 00:00:00'), 1.04, -0.0009, 0.00021, 1.031101001907351,
#              0.06796308070068235, 1.0400611667726307),
#             (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.05, nan, nan, 1.071101001907351,
#              0.06796308070068591, nan),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.06, -0.0071, 0.00021, 1.071101001907351,
#              0.06796308070068591, 1.060482537872975),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.07, -0.0406, 0.00021, 1.071101001907351,
#              0.06796308070068591, 1.072759301076448),
#             (10516, 'b', Timestamp('2000-01-05 00:00:00'), 1.08, -0.0009, 0.00021, 1.071101001907351,
#              0.06796308070068591, 1.0800611667726308),
#             (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.09, nan, nan, 1.111101001907351,
#              0.06796308070068235, nan),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.1, -0.0071, 0.00021, 1.111101001907351,
#              0.06796308070068235, 1.100482537872975),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.11, -0.0406, 0.00021, 1.111101001907351,
#              0.06796308070068235, 1.1127593010764478),
#             (10517, 'a', Timestamp('2000-01-05 00:00:00'), 1.12, -0.0009, 0.00021, 1.111101001907351,
#              0.06796308070068235, 1.1200611667726308),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET', 'mktrf', 'rf', 'const', 'coef_mktrf', 'ABRET'])
#
#         ga = datacode.get_abret(indf, ['PERMNO','byvar'], freq='d', abret_fac=1,
#                                  includecoef=True, includefac=True)
#
#         assert_frame_equal(expect_df, ga)

# TODO [#9]: decouple get CRSP from data paths before getting these tests working

# class TestGetCRSP:
#
#     input_data = DataFrameTest()
#
#
#     crsp = datacode.GetCRSP(debug=True)
#
#
#     def test_get_prc_shrout_same_period_monthly(self):
#         expect_df_prc_shrout_m = pd.DataFrame(data = [
#         (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, 11.75, 608360.0),
#         (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, 11.75, 608360.0),
#         (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, 11.75, 608360.0),
#         (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, 11.75, 608360.0),
#         (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, 11.75, 608360.0),
#         (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, 11.75, 608360.0),
#         (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, 11.75, 608360.0),
#         (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, 11.75, 608360.0),
#         (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, -16.8125, 3830.0),
#         (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, -16.8125, 3830.0),
#         (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -16.8125, 3830.0),
#         (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -16.8125, 3830.0),
#         ], columns = ['PERMNO', 'byvar', 'Date', 'RET', 'PRC', 'SHROUT'])
#
#         gc_prc_shrout_m = self.crsp.pull_crsp(self.input_data.df_datetime) #get PRC and SHROUT is default
#
#         assert_frame_equal(expect_df_prc_shrout_m, gc_prc_shrout_m, check_dtype=False)
#
#     def test_get_prc_shrout_same_period_daily(self):
#         expect_df_prc_shrout_d = pd.DataFrame(data = [
#         (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, nan, nan),
#         (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, nan, nan),
#         (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, 12.0, 608360.0),
#         (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, 11.875, 608360.0),
#         (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, nan, nan),
#         (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, nan, nan),
#         (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, 12.0, 608360.0),
#         (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, 11.875, 608360.0),
#         (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, nan, nan),
#         (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, nan, nan),
#         (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, 17.625, 3830.0),
#         (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, 17.5625, 3830.0),
#         ], columns = ['PERMNO', 'byvar', 'Date', 'RET', 'PRC', 'SHROUT'])
#
#         print('Printing input df: ', self.input_data.df_datetime)
#
#         gc_prc_shrout_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d') #get PRC and SHROUT is default
#
#         print('Printing output df: ', gc_prc_shrout_d)
#
#         assert_frame_equal(expect_df_prc_shrout_d, gc_prc_shrout_d, check_dtype=False)
#
#     def test_get_ret_0_3_monthly(self):
#         expect_df_ret_0_3_m = pd.DataFrame(data = [
#         (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, -0.03092783503234386, -0.036363635212183),
#         (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, -0.03092783503234386, -0.036363635212183),
#         (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.03092783503234386, -0.036363635212183),
#         (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.03092783503234386, -0.036363635212183),
#         (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, -0.03092783503234386, -0.036363635212183),
#         (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, -0.03092783503234386, -0.036363635212183),
#         (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.03092783503234386, -0.036363635212183),
#         (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.03092783503234386, -0.036363635212183),
#         (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, -0.07876712083816527, -0.09854014962911606),
#         (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, -0.07876712083816527, -0.09854014962911606),
#         (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.07876712083816527, -0.09854014962911606),
#         (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.07876712083816527, -0.09854014962911606),
#         ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'RET3'])
#
#         gc_ret_0_3_m = self.crsp.pull_crsp(self.input_data.df_datetime, get=['RET'], time=[0,3],
#                                       other_byvars='byvar') #freq m default
#
#         assert_frame_equal(expect_df_ret_0_3_m, gc_ret_0_3_m, check_dtype=False)
#
#     def test_get_ret_0_3_daily(self):
#         expect_df_ret_0_3_d = pd.DataFrame(data = [
#         (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, -0.010309278033673763, -0.015789473429322243),
#         (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, -0.010309278033673763, -0.015789473429322243),
#         (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.010309278033673763, 0.0053475936874747285),
#         (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.010416666977107523, 0.01595744676887989),
#         (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, -0.010309278033673763, -0.015789473429322243),
#         (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, -0.010309278033673763, -0.015789473429322243),
#         (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.010309278033673763, 0.0053475936874747285),
#         (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.010416666977107523, 0.01595744676887989),
#         (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, -0.034246575087308884, -0.007117437664419413),
#         (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, -0.034246575087308884, -0.007117437664419413),
#         (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.034246575087308884, 0.00358422938734293),
#         (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.0035460991784930225, 0.0),
#         ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'RET3'])
#
#         gc_ret_0_3_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d', get=['RET'],
#                                       other_byvars='byvar', time=[0,3])
#
#         assert_frame_equal(expect_df_ret_0_3_d, gc_ret_0_3_d, check_dtype=False)
#
#     def test_get_ret_m3_0_between_daily(self):
#         expect_df_ret_m3_0_between_d = pd.DataFrame(data = [
#         (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, 0.02105263248085976, 0.0, -0.010309278033673763, -0.010416666977107523),
#         (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.0052356021478772155, 0.02105263248085976, 0.0, -0.010309278033673763),
#         (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, 0.02105263248085976, 0.0, -0.010309278033673763, -0.010416666977107523),
#         (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, 0.003424657508730889, -0.006825938355177641, 0.0034364261664450173, -0.034246575087308884),
#         (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, 0.003424657508730889, -0.006825938355177641, 0.0034364261664450173, -0.034246575087308884),
#         (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, 0.003424657508730889, -0.006825938355177641, 0.0034364261664450173, -0.034246575087308884),
#         (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.006825938355177641, 0.0034364261664450173, -0.034246575087308884, -0.0035460991784930225),
#         ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET-3', 'RET-2', 'RET-1', 'RET'])
#
#         gc_ret_m3_0_between_d = self.crsp.pull_crsp(
#                 self.input_data.df_datetime, get=['RET'], time=[-3,0], between_time=True, other_byvars=['byvar'], freq='d'
#             )
#
#         assert_frame_equal(expect_df_ret_m3_0_between_d, gc_ret_m3_0_between_d, check_dtype=False)
#
#     def test_get_abret_daily(self):
#         expect_df_abret1w30_d = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.010309278033673763, -0.011225175770145413),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.010416666977107523, -0.007502490855258082),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.010309278033673763, -0.011225175770145413),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.010416666977107523, -0.007502490855258082),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.034246575087308884, -0.025145672948656786),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.0035460991784930225, 0.021587078051334464),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'ABRET'])
#
#         expect_df_abret3w30_d = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.010309278033673763, -0.017147875523782847),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.010416666977107523, -0.005220015181566345),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.010309278033673763, -0.017147875523782847),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.010416666977107523, -0.005220015181566345),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.034246575087308884, -0.01889417133577246),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.0035460991784930225, 0.02276294919810182),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'ABRET'])
#
#         expect_df_abret5w30_d = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.010309278033673763, -0.01956387475051293),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.010416666977107523, -0.008912619889327655),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.010309278033673763, -0.01956387475051293),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.010416666977107523, -0.008912619889327655),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.034246575087308884, -0.010109987931638219),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.0035460991784930225, 0.031326305659091905),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'ABRET'])
#
#         gc_abret1w30_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d', get=['RET'],
#                                         other_byvars='byvar', abret=1, window=30)
#         gc_abret3w30_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d', get=['RET'],
#                                         other_byvars='byvar', abret=3, window=30)
#         gc_abret5w30_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d', get=['RET'],
#                                         other_byvars='byvar', abret=5, window=30)
#
#         assert_frame_equal(expect_df_abret1w30_d, gc_abret1w30_d, check_dtype=False)
#         assert_frame_equal(expect_df_abret3w30_d, gc_abret3w30_d, check_dtype=False)
#         assert_frame_equal(expect_df_abret5w30_d, gc_abret5w30_d, check_dtype=False)
#
#     def test_get_abret_includecoef_includefac_daily(self):
#         expect_df_abret1w30ific_d = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.010309278033673763, -0.011225175770145413,
#              -0.0071, 0.00021, -0.1289996811931902, -0.004742858764353774),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.010416666977107523, -0.007502490855258082,
#              -0.0406, 0.00021, 0.07177773699136555, -0.004474063724469225),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.010309278033673763, -0.011225175770145413,
#              -0.0071, 0.00021, -0.1289996811931902, -0.004742858764353774),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.010416666977107523, -0.007502490855258082,
#              -0.0406, 0.00021, 0.07177773699136555, -0.004474063724469225),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.034246575087308884, -0.025145672948656786,
#              -0.0071, 0.00021, 1.2818172026270562, 0.00024156806287732818),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.0035460991784930225, 0.021587078051334464,
#              -0.0406, 0.00021, 0.619043774133682, 0.0011960335526057253),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'ABRET', 'mktrf', 'rf', 'coef_mktrf', 'const'])
#
#         gc_abret1w30ific_d = self.crsp.pull_crsp(self.input_data.df_datetime, freq='d', get=['RET'],
#                                         other_byvars='byvar', abret=1, window=30,
#                                         includefac=True, includecoef=True)
#
#         assert_frame_equal(expect_df_abret1w30ific_d, gc_abret1w30ific_d, check_dtype=False)
#
#     def test_get_abret_monthly(self):
#
#         expect_df_abret5w36_m = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, -0.03092783503234386, -0.02852974253237905),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, -0.03092783503234386, -0.02852974253237905),
#             (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, -0.07876712083816527, -0.05174420878926175),
#             (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, -0.07876712083816527, -0.05174420878926175),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, -0.07876712083816527, -0.05174420878926175),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, -0.07876712083816527, -0.05174420878926175),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET', 'ABRET'])
#
#         gc_abret5w36_m = self.crsp.pull_crsp(self.input_data.df_datetime, get=['RET'],
#                                         other_byvars='byvar', abret=5, window=36)
#
#         assert_frame_equal(expect_df_abret5w36_m, gc_abret5w36_m, check_dtype=False)
#
#     def test_get_abret_0_3_cumretfirst_dropfirst_daily(self):
#         expect_df_cumfirst_abret1w30_0_3_dropf_d = pd.DataFrame(data = [
#             (10516, 'a', Timestamp('2000-01-01 00:00:00'), 1.01, -0.015789473429322243,
#                  -0.0157200164855561, -0.026041666719972656, -0.023104568060886788),
#             (10516, 'a', Timestamp('2000-01-02 00:00:00'), 1.02, -0.015789473429322243,
#                  -0.0157200164855561, -0.026041666719972656, -0.023104568060886788),
#             (10516, 'a', Timestamp('2000-01-03 00:00:00'), 1.03, 0.005347593687474728,
#                  0.00575376791862503, -0.020833333285060984, -0.01748373846474416),
#             (10516, 'a', Timestamp('2000-01-04 00:00:00'), 1.04, 0.01595744676887989,
#                  0.012994269572996098, -0.005208333323095782, -0.0047166573025027025),
#             (10516, 'b', Timestamp('2000-01-01 00:00:00'), 1.05, -0.015789473429322243,
#                  -0.0157200164855561, -0.026041666719972656, -0.023104568060886788),
#             (10516, 'b', Timestamp('2000-01-02 00:00:00'), 1.06, -0.015789473429322243,
#                  -0.0157200164855561, -0.026041666719972656, -0.023104568060886788),
#             (10516, 'b', Timestamp('2000-01-03 00:00:00'), 1.07, 0.005347593687474728,
#                  0.00575376791862503, -0.020833333285060984, -0.01748373846474416),
#             (10516, 'b', Timestamp('2000-01-04 00:00:00'), 1.08, 0.01595744676887989,
#                  0.012994269572996098, -0.005208333323095782, -0.0047166573025027025),
#             (10517, 'a', Timestamp('2000-01-01 00:00:00'), 1.09, -0.007117437664419413,
#                  -0.0065582128902672565, -0.010638297703057686, 0.01488729250752785),
#             (10517, 'a', Timestamp('2000-01-02 00:00:00'), 1.1, -0.007117437664419413,
#                  -0.0065582128902672565, -0.010638297703057686, 0.01488729250752785),
#             (10517, 'a', Timestamp('2000-01-03 00:00:00'), 1.11, 0.00358422938734293,
#                  0.008033133923996294, -0.0070921984149733275, 0.023040018046002864),
#             (10517, 'a', Timestamp('2000-01-04 00:00:00'), 1.12, 0.0, -0.011673811100752163,
#                  -0.0070921984149733275, 0.011097242126823836),
#             ], columns = ['PERMNO', 'byvar', 'Date', 'RET_old', 'RET3',
#                           'ABRET3', 'cum_RET3', 'cum_ABRET3'])
#
#         gc_cumfirst_abret1w30_0_3_dropf_d = self.crsp.pull_crsp(
#                                         self.input_data.df_datetime, freq='d', get=['RET'],
#                                         other_byvars='byvar',time=[0,3], cumret='first', abret=1,
#                                         window=30, drop_first=True)
#
#         assert_frame_equal(expect_df_cumfirst_abret1w30_0_3_dropf_d,
#                            gc_cumfirst_abret1w30_0_3_dropf_d, check_dtype=False)
#
#     def test_on_ticker_get_cumret_between_time_0_3_dropfirst(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('a', Timestamp('2000-01-01 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('a', Timestamp('2000-01-02 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('a', Timestamp('2000-01-03 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('a', Timestamp('2000-01-04 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('b', Timestamp('2000-01-01 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('b', Timestamp('2000-01-02 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('b', Timestamp('2000-01-03 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('b', Timestamp('2000-01-04 00:00:00'), 'ADM', 10516.0, -0.036363635212183, -0.15005286684689223),
#             ('a', Timestamp('2008-01-01 00:00:00'), 'AAN', nan, nan, nan),
#             ('a', Timestamp('2009-01-02 00:00:00'), 'AAN', nan, nan, nan),
#             ('a', Timestamp('2010-01-03 00:00:00'), 'AAN', 78049.0, nan, nan),
#             ('a', Timestamp('2011-01-04 00:00:00'), 'AAN', 10517.0, 0.13525237143039703, 0.5010296116623514),
#             ], columns = ['byvar', 'Date', 'TICKER', 'PERMNO', 'RET3', 'cum_RET3'])
#
#         gc = self.crsp.pull_crsp(self.input_data.ticker_df, coid='TICKER', get='RET', cumret='between',
#                                  time=[0,3], other_byvars='byvar', drop_first=True)
#
#         assert_frame_equal(expect_df, gc, check_dtype=False)

# TODO [#10]: decouple get Compustat from data paths before getting these tests working
# class TestLoadAndMergeCompustat(DataFrameTest):
#
#     def test_freq_a(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('001076', Timestamp('1995-03-01 00:00:00'), Timestamp('1994-03-31 00:00:00'),
#                  185.18400000000003, 112.70299999999999),
#             ('001076', Timestamp('1995-04-01 00:00:00'), Timestamp('1995-03-31 00:00:00'),
#                  228.892, 113.575),
#             ('001722', Timestamp('2012-01-01 00:00:00'), Timestamp('2011-06-30 00:00:00'),
#                  80676.0, 1247.0),
#             ('001722', Timestamp('2012-07-01 00:00:00'), Timestamp('2012-06-30 00:00:00'),
#                  89038.0, 1477.0),
#             ('001722', numpy.timedelta64('NaT','ns'), numpy.timedelta64('NaT','ns'),
#                  numpy.timedelta64('NaT','ns'), numpy.timedelta64('NaT','ns')),
#             (numpy.datetime64('NaT'), numpy.datetime64('2012-01-01T00:00:00.000000000'), numpy.datetime64('NaT'),
#                  numpy.datetime64('NaT'), numpy.datetime64('NaT')),
#             ], columns = ['GVKEY', 'Date', 'datadate', 'sale', 'capx'])
#
#         c_str = datacode.load_and_merge_compustat(self.df_gvkey_str, get=['sale','capx'], freq='a',
#                                                    gvkeyvar='GVKEY', debug=True)
#
#         c_num = datacode.load_and_merge_compustat(self.df_gvkey_num, get=['sale','capx'], freq='a',
#                                                    gvkeyvar='GVKEY', debug=True)
#
#         assert_frame_equal(expect_df, c_str, check_dtype=False)
#         assert_frame_equal(expect_df, c_num, check_dtype=False)
#
#     def test_freq_q(self):
#
#         expect_df = pd.DataFrame(data = [
#             ('001076', Timestamp('1995-03-01 00:00:00'), Timestamp('1994-12-31 00:00:00'),
#                  56.511, 21.96799999999999),
#             ('001076', Timestamp('1995-04-01 00:00:00'), Timestamp('1995-03-31 00:00:00'),
#                  59.551, 29.421000000000006),
#             ('001722', Timestamp('2012-01-01 00:00:00'), Timestamp('2011-12-31 00:00:00'),
#                  23306.0, 409.0),
#             ('001722', Timestamp('2012-07-01 00:00:00'), Timestamp('2012-06-30 00:00:00'),
#                  22675.0, 284.0),
#             ('001722', numpy.timedelta64('NaT','ns'), numpy.timedelta64('NaT','ns'),
#                  numpy.timedelta64('NaT','ns'), numpy.timedelta64('NaT','ns')),
#             (numpy.datetime64('NaT'), numpy.datetime64('2012-01-01T00:00:00.000000000'), numpy.datetime64('NaT'),
#                  numpy.datetime64('NaT'), numpy.datetime64('NaT')),
#             ], columns = ['GVKEY', 'Date', 'datadate', 'saleq', 'capxq'])
#
#
#         c_str = datacode.load_and_merge_compustat(self.df_gvkey_str, get=['sale','capx'], freq='q',
#                                                    gvkeyvar='GVKEY', debug=True)
#
#         c_num = datacode.load_and_merge_compustat(self.df_gvkey_num, get=['sale','capx'], freq='q',
#                                                    gvkeyvar='GVKEY', debug=True)
#
#         assert_frame_equal(expect_df, c_str, check_dtype=False)
#         assert_frame_equal(expect_df, c_num, check_dtype=False)
