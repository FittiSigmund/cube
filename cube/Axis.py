from __future__ import annotations

from typing import List, TYPE_CHECKING

from cube.LevelMemberType import LevelMemberType

if TYPE_CHECKING:
    from cube.NonTopLevel import NonTopLevel
    from cube.LevelMember import LevelMember


class Axis:
    def __init__(self,
                 level: NonTopLevel = None,
                 level_member: List[LevelMember] = None) -> None:
        self.level: NonTopLevel | None = level
        if level_member:
            self.level_members: List[LevelMember] = level_member
            self.type: LevelMemberType = LevelMemberType.STR if type(level_member[0].name) is str else LevelMemberType.INT
        else:
            self.level_members = []

    def __repr__(self) -> str:
        return f"Axis(Level: {self.level}, Level Members: {self.level_members})"