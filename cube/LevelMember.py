import psycopg2
from rdflib import Namespace

from cube.TopLevel import TopLevel

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


def remove_uri_prefix(uri):
    return uri.rsplit("/")[-1]


class LevelMember:
    def __init__(self, name, level, metadata, engine, parent=None):
        self._name = name
        self._children = None
        self._level = level
        self._metadata = metadata
        self._engine = engine
        self._parent = parent

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def level(self):
        return self._level

    def children(self):
        return self._children if self._children is not None else []

    def _get_select_stmt(self):
        return f"SELECT {self._level.child.name}.{self._level.child._level_member_name} "

    def _get_from_stmt(self):
        return f"FROM {self._level.child.name}, {self._level.name}"

    def _get_equality_condition_where_stmt(self, attribute):
        return f"""
            WHERE {self._level.child.name}.{self._level.child._level_member_name} = '{attribute}'
            AND {self._level.name}.{self._level._level_member_name} = '{self.name}'
        """

    def _get_join_condition_where_stmt(self):
        return f"AND {self._level.child.name}.{self._level.child._fk_name} = {self._level.name}.{self._level._pk_name}"

    def get_parents(self):
        levels = []
        current_level = self._level
        while True:
            current_level_parent = current_level.parent
            if current_level == current_level_parent or isinstance(current_level_parent, TopLevel):
                break
            levels.append(current_level_parent)
            current_level = current_level_parent
        return levels

    def _get_query_with_parents(self, attribute, parents):
        select_stmt = self._get_select_stmt()
        from_stmt = self._get_from_stmt()
        equality_conditions = self._get_equality_condition_where_stmt(attribute)
        join_conditions = self._get_join_condition_where_stmt()

        for i in range(len(parents)):
            from_stmt = ", ".join([from_stmt, parents[i].name])
            ## Equality and Join conditions will fail if the hierarchy has more than 3 levels
            equality_conditions = " AND ".join(
                [equality_conditions, f"{parents[i].name}.{parents[i]._level_member_name} = '{self._parent.name}' "])
            join_conditions = " AND ".join(
                [join_conditions,
                 f"{self._level.name}.{self._level._fk_name} = {parents[i].name}.{parents[i]._pk_name}"])

        return select_stmt + from_stmt + equality_conditions + join_conditions

    def _get_query_without_parents(self, attribute):
        select_stmt = self._get_select_stmt()
        from_stmt = self._get_from_stmt()
        equality_conditions = self._get_equality_condition_where_stmt(attribute)
        join_conditions = self._get_join_condition_where_stmt()
        return select_stmt + from_stmt + equality_conditions + join_conditions

    def fetch_level_member_from_db(self, attribute, parents):
        if parents:
            query = self._get_query_with_parents(attribute, parents)
        else:
            query = self._get_query_without_parents(attribute)
        conn = self._get_db_conn()
        with conn.cursor() as curs:
            curs.execute(query)
            result = curs.fetchall()
        conn.close()
        return result

    def _find_parent_node(self):
        parent = self._metadata.query(f"""
            SELECT ?parent
            WHERE {{
                 ?parent qb4o:inLevel eg:{self._level.name} .
                 ?parent rdfs:label eg:{self.name}
            }}
        """)
        if len(parent) == 1:
            for node in parent:
                parent_node = node[0]
            return parent_node
        else:
            raise Exception("Either zero or several parent nodes found in QB4OLAP graph")

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)

    def __getattr__(self, item):
        attribute = remove_underscore_prefix(item)
        parents = self.get_parents()
        result = self.fetch_level_member_from_db(attribute, parents)
        if len(result) == 1:
            level_member_name = result[0][0]
            level_member = LevelMember(level_member_name, self._level.child, self._metadata, self._engine, self)
            setattr(self, item, level_member)
            return level_member
        else:
            raise Exception("There was zero or several results from the query. DB_result: ", result)

    def __getitem__(self, item):
        parents = self.get_parents()
        result = self.fetch_level_member_from_db(item, parents)
        if len(result) == 1:
            level_member_name = result[0][0]
            level_member = LevelMember(level_member_name, self._level.child, self._metadata, self._engine, self)
            setattr(self, item, level_member)
            return level_member
        else:
            raise Exception("There was zero or several results from the query. DB_result: ", result)

    def __repr__(self):
        return f"LevelMember({self.name})"
