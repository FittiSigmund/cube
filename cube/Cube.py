from abc import ABC, abstractmethod


class Cube(ABC):
    def __init__(self, dimension_list, measure_list, engine, previous):
        self._previous = previous
        self._dimension_list = dimension_list
        self._default_measure = measure_list[0]
        self._measure_list = measure_list
        self._engine = engine
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)

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
