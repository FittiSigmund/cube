from __future__ import annotations

from typing import TYPE_CHECKING

from cube.BooleanConnective import BooleanConnective

if TYPE_CHECKING:
    from numbers import Number
    from cube.PredicateOperator import PredicateOperator
    from cube.LevelMember import LevelMember
    from cube.Attribute import Attribute

from cube.LevelMemberType import LevelMemberType


# Change the filter to contain a boolean connective and a pointer to the next filter, if any
# Filter also needs to implement the __and__ and __or__ magic methods
# The __lt__, __lte__, __eq__, and so on comparison methods will be implemented on an Attribute class, which has not
# been implemented yet (the class has not been implemented yet, that is).
class Predicate:
    def __init__(self,
                 attribute: Attribute = None,
                 value: Number | str = None,
                 operator: PredicateOperator | None = None) -> None:
        self.attribute: Attribute | None = attribute
        self.value: Number | str = value
        self.operator: PredicateOperator | None = operator
        self.connective: BooleanConnective = BooleanConnective.EMPTY
        self.next_pred: Predicate | None = None
        match self.value:
            case str():
                self.level_member_type = LevelMemberType.STR
            case int():
                self.level_member_type = LevelMemberType.INT
            case _:
                self.level_member_type = None

    def __and__(self, other):
        current_pred = self
        while current_pred.next_pred is not None:
            current_pred = current_pred.next_pred
        current_pred.connective = BooleanConnective.AND
        current_pred.next_pred = other
        return self

    def __or__(self, other):
        current_pred = self
        while current_pred.next_pred is not None:
            current_pred = current_pred.next_pred
        current_pred.connective = BooleanConnective.OR
        current_pred.next_pred = other
        return self

    def __repr__(self):
        return f"Predicate(Attribute: {self.attribute}, Value: {self.value}, Operator: {self.operator}, Connective: {self.connective})"
