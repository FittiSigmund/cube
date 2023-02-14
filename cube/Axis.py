from __future__ import annotations

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from cube.Level import Level
    from cube.LevelMember import LevelMember


class Axis:
    def __init__(self,
                 level: Level = None,
                 level_member: List[LevelMember] = None) -> None:
        self._level: Level | None = level
        self._level_members: List[LevelMember] = level_member if level_member else []

    def __repr__(self) -> str:
        return f"Axis(Level: {self._level}, Level Members: {self._level_members})"