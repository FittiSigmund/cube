from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numbers import Number
    from cube.FilterOperator import FilterOperator
    from cube.LevelMember import LevelMember
    from cube.NonTopLevel import NonTopLevel

from cube.LevelMemberType import LevelMemberType

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