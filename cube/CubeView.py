from __future__ import annotations

from typing import List

from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.Filter import Filter
from cube.LevelMember import LevelMember
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
        self.cube: BaseCube = cube

    # Checks not implemented
    # All level members same
    # lm contains atleast one member
    # Index is correct
    def axis(self, ax: int, lm: List[LevelMember]) -> CubeView:
        new_axis = Axis(lm[0].level, lm)
        self._axes.insert(ax, new_axis)
        return self

    # The intention is that this method will output a Dataframe.
    # However for development it will only return the SQL resultset returned from the query.
    # Reuse as much as possible from the output method in the BaseCube class
    def output(self) -> str:
        pass