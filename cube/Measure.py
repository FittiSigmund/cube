from __future__ import annotations

from numbers import Number
from typing import Dict

from cube import AggregateFunction
from cube.Predicate import Predicate
from cube.PredicateOperator import PredicateOperator


class Measure:
    def __init__(self, name: str, function: AggregateFunction, sql_name: str):
        self.name: str = name
        self.sqlname: str = sql_name
        self.aggregate_function: AggregateFunction = function

    def aggregate(self, x: Number, y: Number) -> Number:
        return self.aggregate_function.eval(x, y)

    def __mul__(self, other: Measure) -> Dict[str, str | AggregateFunction]:
        return {"function": self.aggregate_function, "sqlname": f"{self.sqlname} * {other.sqlname}"}

    def __repr__(self) -> str:
        return f"Measure({self.name}, {self.aggregate_function})"

    # HACKS
    def __gt__(self, other) -> Predicate:
        return Predicate(self, other, PredicateOperator.GT)

    def __lt__(self, other) -> Predicate:
        return Predicate(self, other, PredicateOperator.LT)
