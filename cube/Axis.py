from __future__ import annotations

from typing import List, TYPE_CHECKING

from cube.Attribute import Attribute
from cube.Dimension import Dimension
from cube.LevelMemberType import LevelMemberType

if TYPE_CHECKING:
    from cube.NonTopLevel import NonTopLevel
    from cube.LevelMember import LevelMember


## Eri í gongd vid at implementera axis metoduna á view classanum
class Axis:
    def __init__(self,
                 dimension: Dimension = None,
                 level: NonTopLevel = None,
                 attribute: Attribute = None,
                 level_member: List[LevelMember] = None) -> None:
        self.dimension = dimension
        self.level: NonTopLevel = level
        self.attribute = attribute
        if level_member:
            self.level_members: List[LevelMember] = level_member
            self.type: LevelMemberType = LevelMemberType.STR if type(level_member[0].name) is str else LevelMemberType.INT
        else:
            self.level_members = []

    def __repr__(self) -> str:
        return f"Axis(Dimension: {self.dimension}, Level: {self.level}, Attribute: {self.attribute}, Level Members: {self.level_members})"