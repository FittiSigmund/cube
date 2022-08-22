from abc import ABC, abstractmethod


class Cube(ABC):
    def __init__(self, dimension_list, measure_list, engine, previous_cube, base_cube, next_cube):
        self._previous = previous_cube
        self._dimension_list = dimension_list
        self._default_measure = measure_list[0]
        self._measure_list = measure_list
        self._engine = engine
        self._base_cube = base_cube
        self._next_cube = next_cube
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)
        for measure in measure_list:
            setattr(self, measure.name, measure)

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
