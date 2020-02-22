from typing import Any, Optional


class AnalysisResult:

    def __init__(self, result: Any = None, name: Optional[str] = None,
                 location: Optional[str] = None):
        self.result = result
        self.name = name
        self.location = location
