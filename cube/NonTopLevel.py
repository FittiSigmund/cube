from typing import List

import psycopg2

from rdflib import Namespace

from cube.Attribute import Attribute
from cube.Level import Level
from engines import Postgres

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


class NonTopLevel(Level):
    def __init__(self, name: str, attributes: List[str], engine: Postgres, key: str, fk: str):
        super().__init__(name, parent=None, child=None, dimension=None)
        self._attributes: List[str] = attributes
        self.key: str = key
        self._fk_name: str = fk
        self._engine: Postgres = engine
        self._metadata = None
        self._level_members = []
        self._all_lm_loaded: bool = False
        self.alias: str = ""

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, value):
        self._child = value

    @property
    def fk_name(self):
        return self._fk_name

    @property
    def all_lm_loaded(self) -> bool:
        return self._all_lm_loaded

    def attributes(self):
        return self._attributes

    # def _append_level_members_without_duplicates(self, lm):
    #     lms_str_list = list(map(lambda x: x.name, self._level_members))
    #     self._level_members.append(lm) if lm.name not in lms_str_list else None

    # def _fetch_all_lm_from_db(self):
    #     conn = self._get_db_conn()
    #     with conn.cursor() as curs:
    #         curs.execute(f"SELECT {self._column_name} FROM {self.name};")
    #         result = curs.fetchall()
    #     conn.close()
    #     return result

    # def create_level_member(self, name):
    #     return LevelMember(name, self, self._metadata, self._engine)


    def __getattr__(self, item):
        if item in self.__dict__.keys():
            return self.__dict__[item]
        elif item in self._attributes:
            attribute = Attribute(item, self._engine, self)
            setattr(self, item, attribute)
            return attribute
        else:
            raise AttributeError(f"Unable to find Attribute {item} on Level {self}")

    # def __getitem__(self, item):
    #     if type(item) is int:
    #         item = int(item)
    #         result = next((x for x in self._level_members if x.name == item), False)
    #     else:
    #         result = next((x for x in self._level_members if x.name == item), False)
    #     if result:
    #         return result
    #     return self._fetch_level_member_from_db_and_save_as_attribute(item)

    # def __eq__(self, other):
    #     def eq():
    #         this_level = self
    #         requirement = other
    #         print(this_level.name)
    #         print(requirement)
    #     return self.name

    def __repr__(self):
        return f"NonTopLevel({self.name})"

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)
