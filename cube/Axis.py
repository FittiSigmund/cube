from typing import List

from cube.Level import Level
from cube.LevelMember import LevelMember


class Axis:
    def __init__(self,
                 level: Level = None,
                 level_member: List[LevelMember] = None) -> None:
        self._level: Level | None = level
        self._level_members: List[LevelMember] = level_member if level_member else []