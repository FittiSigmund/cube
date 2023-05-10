from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numbers import Number
    from cube.FilterOperator import FilterOperator
    from cube.LevelMember import LevelMember
    from cube.NonTopLevel import NonTopLevel

from cube.LevelMemberType import LevelMemberType

# Change the filter to contain a boolean connective and a pointer to the next filter, if any
# Filter also needs to implement the __and__ and __or__ magic methods
# The __lt__, __lte__, __eq__, and so on comparison methods will be implemented on an Attribute class, which has not
# been implemented yet (the class has not been implemented yet, that is).
class Filter:
    def __init__(self,
                 level: NonTopLevel = None,
                 value: LevelMember | Number = None,
                 operator: FilterOperator | None = None) -> None:
        self.level: NonTopLevel | None = level
        self.value = value
        self.operator = operator
        match value.name:
            case str():
                self.level_member_type = LevelMemberType.STR
            case int():
                self.level_member_type = LevelMemberType.INT
            case _:
                self.level_member_type = None