from __future__ import annotations
from typing import Optional, List, Union, Dict, TYPE_CHECKING

from cube.Cube import Cube
from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from engines import Postgres

if TYPE_CHECKING:
    from cube.CubeOperators import rollup, dice, generate_cube
    from cube.BaseCube import BaseCube
from cube.LevelMember import LevelMember
from cube.NonTopLevel import NonTopLevel


class Cuboid(Cube):
    def __init__(self,
                 dimension_list: List[RegularDimension],
                 measure_list: List[Measure],
                 engine: Postgres,
                 base_cube: BaseCube,
                 visual_column: NonTopLevel | None = None,
                 column_value_list: List[NonTopLevel] | None = None,
                 visual_row: NonTopLevel | None = None,
                 row_value_list: List[NonTopLevel] | None = None):
        super().__init__(dimension_list, measure_list, engine, base_cube=base_cube, next_cube=None)
        self._previous: Cube | None = None
        self._visual_column: NonTopLevel | None = visual_column
        self._column_value_list: List[LevelMember] | None = column_value_list
        self._visual_row: NonTopLevel | None = visual_row
        self._row_value_list: List[LevelMember] | None = row_value_list

    @property
    def previous(self):
        return self._previous

    @previous.setter
    def previous(self, value):
        self._previous = value

    @property
    def visual_column(self) -> NonTopLevel | None:
        return self._visual_column

    @visual_column.setter
    def visual_column(self, value: NonTopLevel) -> None:
        self._visual_column: NonTopLevel = value

    @property
    def column_value_list(self) -> List[LevelMember] | None:
        return self._column_value_list

    @column_value_list.setter
    def column_value_list(self, value) -> None:
        self._column_value_list = value

    @property
    def visual_row(self) -> NonTopLevel | None:
        return self._visual_row

    @visual_row.setter
    def visual_row(self, value: NonTopLevel) -> None:
        self._visual_row = value

    @property
    def row_value_list(self) -> List[LevelMember] | None:
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
            from cube.CubeOperators import rollup, dice
            dimension_name: Union[str, int] = value_list[0].level.dimension.name
            level_name: Union[str, int] = value_list[0].level.name
            kwargs: Dict[Union[str, int], Union[str, int]] = {dimension_name: level_name}
            cube1: Cuboid = rollup(self, **kwargs)
            cube2: Cuboid = dice(cube1, value_list, "row")
            if self._column_value_list:
                cube2.column_value_list = self._column_value_list
                cube2.visual_column = self.visual_column
            cube2.row_value_list = value_list
            cube2.visual_row = value_list[0].level
            self.base_cube.next_cube = cube2
            return cube2


    def pages(self, value_list):
        pass

    def where(self):
        pass

    def output(self):
        cube: Cuboid = generate_cube(self)
        return cube.previous.output()
        # if isinstance(self._previous, BaseCube):
        #     return self._previous.output()
        # else:
        #     raise NotImplementedError("Only output for BaseCube is implemented")
            # prev_results = self._previous.output()
            # column_list_names = self._get_column_list_names()
            # result = prev_results[column_list_names]
            # return result

    def __repr__(self):
        return "Cuboid"
