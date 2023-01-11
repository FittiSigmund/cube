from numbers import Number

from cube import AggregateFunction


class Measure:
    def __init__(self, name: str, function: AggregateFunction):
        self._name: str = name
        self._aggregate_function: AggregateFunction = function

    def aggregate(self, x: Number, y: Number) -> Number:
        return self._aggregate_function.eval(x, y)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def aggregate_function(self) -> AggregateFunction:
        return self._aggregate_function

    def __repr__(self) -> str:
        return self.name
