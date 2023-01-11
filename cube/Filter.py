from numbers import Number

from cube.FilterOperator import FilterOperator
from cube.Level import Level
from cube.LevelMember import LevelMember


class Filter:
    def __init__(self,
                 level: Level = None,
                 value: LevelMember | Number = None,
                 operator: FilterOperator | None = None) -> None:
        self._level: Level | None = level
        self._value = value
        self._operator = operator