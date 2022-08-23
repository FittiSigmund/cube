from __future__ import annotations
from typing import Optional, List, Union, Dict, TYPE_CHECKING

from cube.Cube import Cube
if TYPE_CHECKING:
    from cube.CubeOperators import rollup, dice
from cube.LevelMember import LevelMember
from cube.NonTopLevel import NonTopLevel


class Cuboid(Cube):
    def __init__(self, dimension_list, measure_list, engine, previous_cube, base_cube):
        super().__init__(dimension_list, measure_list, engine, previous_cube, base_cube=base_cube, next_cube=None)
        self._visual_column: Optional[NonTopLevel] = None
        self._column_value_list: Optional[List[LevelMember]] = None
        self._visual_row: Optional[NonTopLevel] = None
        self._row_value_list: Optional[List[LevelMember]] = None

    @property
    def visual_column(self) -> Optional[NonTopLevel]:
        return self._visual_column

    @visual_column.setter
    def visual_column(self, value: NonTopLevel) -> None:
        self._visual_column: NonTopLevel = value

    @property
    def column_value_list(self) -> Optional[List[LevelMember]]:
        return self._column_value_list

    @column_value_list.setter
    def column_value_list(self, value) -> None:
        self._column_value_list = value

    @property
    def visual_row(self) -> Optional[NonTopLevel]:
        return self._visual_row

    @visual_row.setter
    def visual_row(self, value: NonTopLevel) -> None:
        self._visual_row = value

    @property
    def row_value_list(self) -> Optional[List[LevelMember]]:
        return self._row_value_list

    @row_value_list.setter
    def row_value_list(self, value: List[LevelMember]) -> None:
        self._row_value_list = value

    def columns(self, value_list):
        # cube = Cuboid(
        #     self._dimension_list,
        #     self._measure_list,
        #     self._engine,
        #     self,
        #     self._base_cube,
        #     columns=value_list
        # )
        # self.next_cube = cube
        # return cube
        raise NotImplementedError("Not implemented columns for Cuboids, only BaseCubes, "
                                  "because I'm changing the way the columns attribute is used.")

    def rows(self, value_list: List[LevelMember]) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            dimension_name: Union[str, int] = value_list[0].level.dimension.name
            level_name: Union[str, int] = value_list[0].level.name
            kwargs: Dict[Union[str, int], Union[str, int]] = {dimension_name: level_name}
            cube1: Cuboid = rollup(self, **kwargs)
            cube2: Cuboid = dice(cube1, value_list, "row")


    def pages(self, value_list):
        pass

    def where(self):
        pass

    def output(self):
        from cube.BaseCube import BaseCube
        if isinstance(self._previous, BaseCube):
            return self._previous.output()
        else:
            raise NotImplementedError("Only output for BaseCube is implemented")
            # prev_results = self._previous.output()
            # column_list_names = self._get_column_list_names()
            # result = prev_results[column_list_names]
            # return result

    def __repr__(self):
        return "Cuboid"
