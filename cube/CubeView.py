from typing import List, Any

from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.Filter import Filter
from cube.Measure import Measure


class CubeView:
    def __init__(self,
                 axes: List[Axis] = None,
                 measures: List[Measure] = None,
                 filters: List[Filter] = None,
                 cube: BaseCube | None = None) -> None:
        self._axes: List[Axis] = axes if axes else []
        self._measures: List[Measure] = measures if measures else []
        self._filters: List[Filter] = filters if filters else []
        self._cube: BaseCube = cube
