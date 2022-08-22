from typing import Optional, List

from cube.Cube import Cube
from cube.LevelMember import LevelMember
from cube.NonTopLevel import NonTopLevel


class Cuboid(Cube):
    def __init__(self, dimension_list, measure_list, engine, previous_cube, base_cube):
        super().__init__(dimension_list, measure_list, engine, previous_cube, base_cube=base_cube, next_cube=None)
        self._visual_column: Optional[NonTopLevel] = None
        self._column_value_list: Optional[List[LevelMember]] = None

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

    def rows(self, value_list):
        pass

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
