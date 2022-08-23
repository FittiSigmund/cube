from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional

from cube.Measure import Measure


class Cube(ABC):
    def __init__(self, dimension_list, measure_list, engine, previous_cube, base_cube, next_cube):
        self._previous = previous_cube
        self._dimension_list = dimension_list
        self._measure_list = measure_list
        self._default_measure = measure_list[0]
        self._engine = engine
        self._base_cube = base_cube
        self._next_cube = next_cube
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)
        for measure in measure_list:
            setattr(self, measure.name, measure)
        self._temp_measure: Optional[Measure] = None
        self._use_temp_measure: bool = False

    @property
    def previous(self):
        return self._previous

    @previous.setter
    def previous(self, value):
        self._previous = value

    @property
    def dimension_list(self):
        return self._dimension_list

    @property
    def measure_list(self):
        return self._measure_list

    @property
    def default_measure(self):
        return self._default_measure

    @default_measure.setter
    def default_measure(self, value):
        self._default_measure = value

    @property
    def engine(self):
        return self._engine

    @property
    def base_cube(self):
        return self._base_cube

    @base_cube.setter
    def base_cube(self, value):
        self._base_cube = value

    @property
    def next_cube(self):
        return self._next_cube

    @next_cube.setter
    def next_cube(self, value):
        self._next_cube = value

    @property
    def use_temp_measure(self) -> bool:
        return self._use_temp_measure

    @use_temp_measure.setter
    def use_temp_measure(self, value: bool) -> None:
        self._use_temp_measure = value

    @property
    def temp_measure(self) -> Optional[Measure]:
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
        self._temp_measure = measure
        self._use_temp_measure = True
        return self

