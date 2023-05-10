from __future__ import annotations
from typing import Optional, List, Union, Dict, TYPE_CHECKING

from cube.Cube import Cube
from cube.Measure import Measure
from cube.Dimension import Dimension
from engines import Postgres

if TYPE_CHECKING:
    from cube.CubeOperators import rollup, dice, generate_cube
    from cube.BaseCube import BaseCube
from cube.LevelMember import LevelMember
from cube.NonTopLevel import NonTopLevel


class Cuboid(Cube):
    def __init__(self,
                 dimension_list: List[Dimension],
                 measure_list: List[Measure],
                 engine: Postgres,
                 base_cube: BaseCube,
                 visual_column: NonTopLevel | None = None,
                 column_value_list: List[LevelMember] | None = None,
                 visual_row: NonTopLevel | None = None,
                 row_value_list: List[LevelMember] | None = None):
        super().__init__(dimension_list, measure_list, engine, base_cube=base_cube, next_cube=None)
        self._previous: Cube | None = None
        self._visual_column: NonTopLevel | None = visual_column
        self._column_value_list: List[LevelMember] | None = column_value_list
        self._visual_row: NonTopLevel | None = visual_row
        self._row_value_list: List[LevelMember] | None = row_value_list
        self._is_generated: bool = False

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

    @property
    def is_generated(self) -> bool:
        return self._is_generated

    @is_generated.setter
    def is_generated(self, value: bool) -> None:
        self._is_generated = value

    def columns(self, value_list):
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            c = Cuboid(self.dimension_list,
                       self.measure_list,
                       self.engine,
                       self.base_cube,
                       visual_column=value_list[0].level,
                       column_value_list=value_list,
                       visual_row=self.visual_row,
                       row_value_list=self.row_value_list)
            c.previous = self.previous
            return c

    def rows(self, value_list: List[LevelMember]) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            c = Cuboid(self.dimension_list,
                       self.measure_list,
                       self.engine,
                       self.base_cube,
                       visual_column=self.visual_column,
                       column_value_list=self.column_value_list,
                       visual_row=value_list[0].level,
                       row_value_list=value_list)
            c.previous = self.previous
            return c

    def pages(self, value_list):
        pass

    def where(self):
        pass

    def output(self):
        if not self.is_generated:
            from cube.CubeOperators import generate_cube
            cube: Cuboid = generate_cube(self)
            self.is_generated = True
            return cube.previous.output()
        else:
            return self.previous.output()

    def __repr__(self):
        return "Cuboid"
