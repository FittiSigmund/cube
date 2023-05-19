from __future__ import annotations
from typing import TYPE_CHECKING, List, Tuple

from cube.PredicateOperator import PredicateOperator
from cube.Predicate import Predicate

if TYPE_CHECKING:
    from cube.NonTopLevel import NonTopLevel

import psycopg2
from cube.LevelMember import LevelMember
from engines import Postgres


class Attribute:
    def __init__(self,
                 name: str,
                 engine: Postgres,
                 level: NonTopLevel):
        self.name: str = name
        self._engine: Postgres = engine
        self.level: NonTopLevel = level
        self.level_members: List[LevelMember] = []

    def members(self):
        with self._get_db_conn() as conn:
            with conn.cursor() as curs:
                curs.execute(f"""
                    SELECT {self.name}
                    FROM {self.level.name}
                """)
                db_result: List[Tuple[str, ...]] = curs.fetchall()
        if db_result:
            return list(map(lambda x: LevelMember(x[0], self), db_result))
        else:
            raise AttributeError(f"The '{self.name}' Attribute does not contain any Level Members")

    def __getattr__(self, item: str):
        if item in self.__dict__["level_members"]:
            return self._fetch_lm_from_db_and_save(item)
        else:
            raise AttributeError(f"Level Member {item} was not found on Attribute {self}")

    def __getitem__(self, item: str | int):
        if item in self.level_members:
            return self.level_members[item]
        else:
            return self._fetch_lm_from_db_and_save(str(item))

    def __eq__(self, other) -> Predicate:
        return Predicate(self, other, PredicateOperator.EQ)

    def __gt__(self, other) -> Predicate:
        return Predicate(self, other, PredicateOperator.GT)

    def __lt__(self, other) -> Predicate:
        return Predicate(self, other, PredicateOperator.LT)

    def _fetch_lm_from_db_and_save(self, item: str):
        with self._get_db_conn() as conn:
            with conn.cursor() as curs:
                curs.execute(f"""
                    SELECT {self.name}
                    FROM {self.level.name}
                    WHERE {self.name} = '{item}'
                """)
                db_result = curs.fetchall()

        if db_result:
            level_member = LevelMember(db_result[0][0], self)
            setattr(self, str(item), level_member)
            self.level_members.append(level_member)
            return level_member
        else:
            raise AttributeError(f"'Unable to find the '{item}' Level Member on the {self.name} Attribute")

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)

    def __repr__(self):
        return f"Attribute({self.name})"
