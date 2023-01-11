from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

from cube import RegularDimension
from cube.Measure import Measure
from engines import Postgres


class Cube(ABC):
    def __init__(self,
                 dimension_list: List[RegularDimension],
                 measure_list: List[Measure],
                 engine: Postgres,
                 base_cube: Cube | None,
                 next_cube: Cube | None):
        self._dimension_list: List[RegularDimension] = dimension_list
        self._measure_list: List[Measure] = measure_list
        self._default_measure: Measure = measure_list[0]
        self._engine: Postgres = engine
        self._base_cube: Cube | None = base_cube
        self._next_cube: Cube | None = next_cube
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)
        for measure in measure_list:
            setattr(self, measure.name, measure)
        self._temp_measure: Measure | None = None
        self._use_temp_measure: bool = False


    @property
    def dimension_list(self) -> List[RegularDimension]:
        return self._dimension_list

    @property
    def measure_list(self) -> List[Measure]:
        return self._measure_list

    @property
    def default_measure(self) -> Measure:
        return self._default_measure

    @default_measure.setter
    def default_measure(self, value: Measure) -> None:
        self._default_measure = value

    @property
    def engine(self) -> Postgres:
        return self._engine

    @property
    def base_cube(self) -> Cube | None:
        return self._base_cube

    @base_cube.setter
    def base_cube(self, value: Cube) -> None:
        self._base_cube = value

    @property
    def next_cube(self) -> Cube | None:
        return self._next_cube

    @next_cube.setter
    def next_cube(self, value: Cube) -> None:
        self._next_cube = value

    @property
    def use_temp_measure(self) -> bool:
        return self._use_temp_measure

    @use_temp_measure.setter
    def use_temp_measure(self, value: bool) -> None:
        self._use_temp_measure = value

    @property
    def temp_measure(self) -> Measure | None:
        return self._temp_measure

    @abstractmethod
    def columns(self, value_list):
        pass

    @abstractmethod
    def rows(self, value_list):
        pass

    @abstractmethod
    def pages(self, value_list):
        pass

    @abstractmethod
    def where(self):
        pass

    @abstractmethod
    def output(self):
        pass

    def with_measures(self, measure: Measure) -> Cube:
        self._temp_measure, self._use_temp_measure = measure, True
        return self

