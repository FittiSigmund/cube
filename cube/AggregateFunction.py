from numbers import Number
from typing import Callable


class AggregateFunction:
    def __init__(self, name: str, function: Callable[[Number, Number], Number]):
        self._name: str = name
        self._function: Callable[[Number, Number], Number] = function

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    def eval(self, x: Number, y: Number) -> Number:
        return self._function(x, y)

    def __repr__(self) -> str:
        return self.name
