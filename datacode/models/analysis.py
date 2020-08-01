import datetime
from typing import Any, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from datacode.models.pipeline.base import DataPipeline

from mixins import ReprMixin


class AnalysisResult(ReprMixin):
    repr_cols = ['name', 'location', 'result']

    def __init__(self, result: Any = None, name: Optional[str] = None,
                 location: Optional[str] = None, last_modified: Optional[datetime.datetime] = None,
                 pipeline: Optional['DataPipeline'] = None):
        self.result = result
        self.name = name
        self.location = location
        self.last_modified = last_modified
        self.pipeline = pipeline
