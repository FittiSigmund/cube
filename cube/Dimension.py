from abc import ABC
from typing import List

from cube.Level import Level


class Dimension(ABC):
    def __init__(self, name: str, level_list: List[Level]):
        self._name: str = name
        self.level_list: List[Level] = level_list

    @property
    def name(self) -> str:
        return self._name
