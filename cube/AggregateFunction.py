from numbers import Number
from typing import Callable


class AggregateFunction:
    def __init__(self, name: str, function: Callable[[Number, Number], Number]):
        self.name: str = name
        self._function: Callable[[Number, Number], Number] = function

    def eval(self, x: Number, y: Number) -> Number:
        return self._function(x, y)

    def __repr__(self) -> str:
        return f"AggregateFunction({self.name})"
