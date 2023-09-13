from typing import List, TypeVar, Union

import psycopg2
from psycopg2.extensions import cursor as psycur
from rdflib import Graph

from cube.Level import Level
from cube.NonTopLevel import NonTopLevel
from cube.TopLevel import TopLevel
from engines import Postgres

level = TypeVar("level", bound=Level)

def get_db_cursor(engine: Postgres) -> psycur:
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()


class Dimension:
    def __init__(self,
                 name: str,
                 level_list: List[level],
                 engine: Postgres,
                 fact_table_fk: str):
        self._name: str = name
        self.level_list: List[Level] = level_list
        self._lowest_level: NonTopLevel = level_list[0]
        self.engine: Postgres = engine
        self._cursor: psycur = get_db_cursor(engine)
        self._metadata: Union[Graph, None] = None
        self.fact_table_fk: str = fact_table_fk
        for level in level_list:
            level.dimension = self
            if not isinstance(level, TopLevel):
                setattr(self, level.name, level)

    def lowest_level(self) -> NonTopLevel:
        return self._lowest_level

    @property
    def name(self) -> str:
        return self._name

    @property
    def metadata(self) -> Union[Graph, None]:
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Graph) -> None:
        self._metadata: Graph = metadata
        for level in self.level_list:
            level._metadata = metadata

    def hierarchies(self):
        current_level = self.lowest_level()
        hierarchy = [current_level]
        while current_level != current_level.parent:
            current_level = current_level.parent
            hierarchy.append(current_level)
        return hierarchy

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
