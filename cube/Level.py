import psycopg2

from rdflib import BNode, Namespace
from rdflib.namespace import RDFS
from cube.LevelMember import LevelMember

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


class Level:
    def __init__(self, name, member_name, engine, pk, fk):
        self._name = name
        self._member_name = member_name
        self._pk_name = pk
        self._fk_name = fk
        self._cursor = get_db_cursor(engine)
        # Initialize parent to None first in order to set it later using the parent.setter
        # If I didn't do this, I would have a never ending chain of Level initializations
        self._parent = None
        self._child = None
        self._metadata = None
        self._dimension = None

    def members(self):
        return self.__level_member_values

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        self._parent = value

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, value):
        self._child = value

    def fetch_attribute_from_db(self, attribute):
        self._cursor.execute(f"""
            SELECT {self._member_name}
            FROM {self.name}
            WHERE {self._member_name} = '{attribute}';
        """)
        return self._cursor.fetchall()[0][0]

    def create_level_member(self, name):
        return LevelMember(name, self, self._metadata)

    def add_level_member_metadata(self, level_member_value):
        level_member_node = BNode()
        self._metadata.add((level_member_node, QB4O.inLevel, EG[self.name]))
        self._metadata.add((level_member_node, RDFS.label, EG[level_member_value]))

    def __getattr__(self, item):
        attribute = remove_underscore_prefix(item)
        db_result = self.fetch_attribute_from_db(attribute)

        if db_result:
            level_member = self.create_level_member(db_result)
            setattr(self, item, level_member)
            self.add_level_member_metadata(attribute)
            return level_member
        else:
            return "EXCEPTIONS NOT IMPLEMENTED EXCEPTION"

    def __repr__(self):
        return self.name


def get_db_cursor(engine):
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()
