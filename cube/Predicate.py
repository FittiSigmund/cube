from __future__ import annotations

import numbers
from typing import TYPE_CHECKING

from cube.BooleanConnective import BooleanConnective

if TYPE_CHECKING:
    from numbers import Number
    from cube.PredicateOperator import PredicateOperator
    from cube.Attribute import Attribute
    from cube.Measure import Measure

from cube.LevelMemberType import LevelMemberType


class Predicate:
    def __init__(self,
                 left_child: Predicate | None,
                 value: PredicateOperator | BooleanConnective | Attribute | Measure | Number | str,
                 right_child: Predicate | None) -> None:
        self.left_child: Predicate | None = left_child
        self.value: PredicateOperator | BooleanConnective | Attribute | Number | str = value
        self.right_child: Predicate | None = right_child
        match self.value:
            case numbers.Number():
                self.level_member_type = LevelMemberType.INT
            case str():
                self.level_member_type = LevelMemberType.STR
            case _:
                self.level_member_type = None

    def __and__(self, other):
        return Predicate(self, BooleanConnective.AND, other)

    def __or__(self, other):
        return Predicate(self, BooleanConnective.OR, other)

    def __repr__(self):
        return f"Predicate(left_child = {self.left_child}, value = {self.value}, right_child = {self.right_child})"