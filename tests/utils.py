import os

from pandas.testing import assert_frame_equal

GENERATED_PATH = os.path.join('tests', 'generated_files')

def assert_frame_not_equal(*args, **kwargs):
    try:
        assert_frame_equal(*args, **kwargs)
    except AssertionError:
        # frames are not equal
        pass
    else:
        # frames are equal
        raise AssertionError