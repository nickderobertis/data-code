from typing import Union, Optional

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.bool_type import BooleanType
from datacode.models.dtypes.datetime_type import DatetimeType
from datacode.models.dtypes.float_type import FloatType
from datacode.models.dtypes.int_type import IntType
from datacode.models.dtypes.manager import DataTypeManager
from datacode.models.dtypes.obj_type import ObjectType
from datacode.models.dtypes.str_type import StringType
from datacode.models.dtypes.timedelta_type import TimedeltaType


def convert_str_to_data_type_if_necessary(dtype: Union[str, DataType]) -> Optional[DataType]:
    if isinstance(dtype, DataType):
        return dtype

    if dtype is None:
        return None

    return convert_str_to_data_type(dtype)


def convert_str_to_data_type(dtype: str) -> DataType:
    manager = DataTypeManager([
        BooleanType(),
        DatetimeType(),
        FloatType(),
        IntType(),
        ObjectType(),
        StringType(),
        TimedeltaType(),
    ])
    return manager.get_by_name(dtype)
