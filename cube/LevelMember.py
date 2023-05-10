from __future__ import annotations
from typing import List, Union, Optional, TYPE_CHECKING

import psycopg2
from rdflib import Namespace


if TYPE_CHECKING:
    from cube.NonTopLevel import NonTopLevel
    from psycopg2 import connection
    from cube.Attribute import Attribute
from cube.TopLevel import TopLevel

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


def remove_uri_prefix(uri):
    return uri.rsplit("/")[-1]


def _construct_query(select_stmt: str, from_stmt: str, join: str, eq: str) -> str:
    return f"SELECT {select_stmt} FROM {from_stmt} WHERE {join} AND {eq};"


class LevelMember:
    def __init__(self,
                 name: str | int | float,
                 attribute: Attribute,
                 parent: LevelMember = None):
        self._name: str | int | float = name
        self._children: List[LevelMember] = []
        self._attribute: Attribute = attribute
        self._parent: Optional[LevelMember] = parent

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def attribute(self):
        return self._attribute

    @property
    def parent(self) -> LevelMember:
        return self._parent

    @parent.setter
    def parent(self, value: LevelMember):
        self._parent = value

    def children(self) -> List[LevelMember]:
        if self._children:
            return self._children
        else:
            children = self._fetch_children_from_db()
            if children:
                for child in children:
                    lm_name = child[0]
                    lm = LevelMember(lm_name, self.attribute.child, self)
                    setattr(self.attribute.child, str(lm_name), lm)
                    self._children.append(lm)
                return self._children
            else:
                raise AttributeError(f"{self.name} does not have any children")

    def __getattr__(self, item) -> LevelMember:
        result: LevelMember = getattr(self.attribute.child, item)
        if result:
            result.parent = self
            return result
        else:
            return self._get_attr_or_item(item)

    def __getitem__(self, item) -> LevelMember:
        return self._get_attr_or_item(item)

    def __repr__(self):
        return f"LevelMember({self.name})"

    def _fetch_children_from_db(self):
        select_stmt = self._get_select_stmt_for_children()
        from_stmt = self._get_from_stmt_for_children()
        join_condition = self._get_join_condition_where_stmt_for_children()
        equality_condition = self._get_equality_condition_where_stmt_for_children()
        query = _construct_query(select_stmt, from_stmt, join_condition, equality_condition)

        conn = self._get_db_conn()
        with conn.cursor() as curs:
            curs.execute(query)
            result = curs.fetchall()
        conn.close()

        return result

    def _get_equality_condition_where_stmt_for_children(self) -> str:
        return f"{self.attribute.name}.{self.attribute.column_name} = {self.name}"

    def _get_join_condition_where_stmt_for_children(self) -> str:
        return f"{self.attribute.name}.{self.attribute.pk_name} = {self.attribute.child.name}.{self.attribute.child.fk_name}"

    def _get_from_stmt_for_children(self) -> str:
        return f"{self.attribute.name}, {self.attribute.child.name}"

    def _get_select_stmt_for_children(self) -> str:
        return f"{self.attribute.child.name}.{self.attribute.child.column_name}"

    def _get_ancestors(self) -> List[NonTopLevel]:
        levels = []
        current_level = self.attribute
        while True:
            current_level_parent = current_level.parent
            if current_level == current_level_parent or isinstance(current_level_parent, TopLevel):
                break
            levels.append(current_level_parent)
            current_level = current_level_parent
        return levels

    def _get_select_stmt_for_getattr_or_getitem(self) -> str:
        return f"SELECT {self.attribute.child.name}.{self.attribute.child._column_name} "

    def _get_from_stmt_for_getattr_or_getitem(self):
        return f"FROM {self.attribute.child.name}, {self.attribute.name} "

    def _get_equality_condition_where_stmt(self, attribute: Union[str, int]):
        return f"WHERE {self.attribute.child.name}.{self.attribute.child._column_name} = '{attribute}' AND {self.attribute.name}.{self.attribute._column_name} = '{self.name}' "

    def _get_join_condition_where_stmt(self):
        return f"AND {self.attribute.child.name}.{self.attribute.child._fk_name} = {self.attribute.name}.{self.attribute._pk_name}"

    def _get_query_with_parents(self, attribute: Union[str, int], parents: List[NonTopLevel]) -> str:
        select_stmt: str = self._get_select_stmt_for_getattr_or_getitem()
        from_stmt: str = self._get_from_stmt_for_getattr_or_getitem()
        equality_conditions: str = self._get_equality_condition_where_stmt(attribute)
        join_conditions: str = self._get_join_condition_where_stmt()

        for i in range(len(parents)):
            from_stmt: str = ", ".join([from_stmt, parents[i].name])
            ## Equality and Join conditions will fail if the hierarchy has more than 3 levels
            equality_conditions: str = " AND ".join(
                [equality_conditions, f"{parents[i].name}.{parents[i]._column_name} = '{self._parent.name}' "])
            join_conditions: str = " AND ".join(
                [join_conditions,
                 f"{self.attribute.name}.{self.attribute._fk_name} = {parents[i].name}.{parents[i]._pk_name}"])

        return select_stmt + from_stmt + " " + equality_conditions + join_conditions

    def _get_query_without_parents(self, attribute: Union[str, int]) -> str:
        select_stmt: str = self._get_select_stmt_for_getattr_or_getitem()
        from_stmt: str = self._get_from_stmt_for_getattr_or_getitem()
        equality_conditions: str = self._get_equality_condition_where_stmt(attribute)
        join_conditions: str = self._get_join_condition_where_stmt()
        return select_stmt + from_stmt + equality_conditions + join_conditions

    def _fetch_level_member_from_db(self, attribute: Union[str, int], parents: List[NonTopLevel]):
        if parents:
            query: str = self._get_query_with_parents(attribute, parents)
        else:
            query = self._get_query_without_parents(attribute)
        conn: connection = self._get_db_conn()
        with conn.cursor() as curs:
            curs.execute(query)
            result = curs.fetchall()
        conn.close()
        return result

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)

    def _get_attr_or_item(self, item: Union[str, int]) -> LevelMember:
        parents: List[NonTopLevel] = self._get_ancestors()
        result = self._fetch_level_member_from_db(item, parents)
        if len(result) == 1:
            level_member_name: str = result[0][0]
            level_member: LevelMember = LevelMember(level_member_name, self.attribute.child, self)
            setattr(self, item, level_member)
            return level_member
        else:
            raise Exception("There was zero or several results from the query. DB_result: ", result)
