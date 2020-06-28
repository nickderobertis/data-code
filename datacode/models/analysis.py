from typing import Any, Optional

from mixins import ReprMixin


class AnalysisResult(ReprMixin):
    repr_cols = ['name', 'location', 'result']

    def __init__(self, result: Any = None, name: Optional[str] = None,
                 location: Optional[str] = None):
        self.result = result
        self.name = name
        self.location = location
