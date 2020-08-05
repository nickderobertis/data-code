import os

from pandas.testing import assert_frame_equal

import datacode as dc
import datacode.hooks as dc_hooks
from datacode.models.pipeline.operations.analysis import AnalysisOperation
from datacode.models.pipeline.operations.combine import CombineOperation
from datacode.models.pipeline.operations.generate import GenerationOperation
from datacode.models.pipeline.operations.load import LoadOperation
from datacode.models.pipeline.operations.merge import DataMerge
from datacode.models.pipeline.operations.operation import DataOperation
from datacode.models.pipeline.operations.transform import TransformOperation

GENERATED_PATH = os.path.join('tests', 'generated_files')
AUTO_CACHE_PATH = os.path.join(GENERATED_PATH, 'autocache')
INPUT_FILES_PATH = os.path.join('tests', 'input_files')

def assert_frame_not_equal(*args, **kwargs):
    try:
        assert_frame_equal(*args, **kwargs)
    except AssertionError:
        # frames are not equal
        pass
    else:
        # frames are equal
        raise AssertionError


OPERATION_COUNTER = dict(
    transform=0,
    generator=0,
    combine=0,
    merge=0,
    load=0,
    analysis=0
)


def count_operation(operation: DataOperation) -> None:
    global OPERATION_COUNTER
    class_map = {
        TransformOperation: 'transform',
        GenerationOperation: 'generator',
        CombineOperation: 'combine',
        DataMerge: 'merge',
        LoadOperation: 'load',
        AnalysisOperation: 'analysis'
    }
    for klass, key in class_map.items():
        if isinstance(operation, klass):
            OPERATION_COUNTER[key] += 1


def start_count_operation_hook():
    dc_hooks.on_begin_execute_operation = count_operation


def reset_operation_counter():
    global OPERATION_COUNTER
    OPERATION_COUNTER = dict(
        transform=0,
        generator=0,
        combine=0,
        merge=0,
        load=0,
        analysis=0
    )