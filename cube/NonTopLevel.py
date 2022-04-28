import psycopg2

from rdflib import BNode, Namespace
from rdflib.namespace import RDFS

from cube.Level import Level
from cube.LevelMember import LevelMember

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


class NonTopLevel(Level):
    def __init__(self, name, member_name, engine, pk, fk):
        super().__init__(name, parent=None, child=None, dimension=None)
        self._member_name = member_name
        self._pk_name = pk
        self._fk_name = fk
        self._cursor = get_db_cursor(engine)
        self._metadata = None
        self._member_values = None

    def members(self):
        raise NotImplementedError("Lazy version not implemented")

    def fetch_attribute_from_db(self, attribute):
        self._cursor.execute(f"""
            SELECT {self._member_name}
            FROM {self.name}
            WHERE {self._member_name} = '{attribute}';
        """)
        return self._cursor.fetchall()

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
            level_member = self.create_level_member(db_result[0][0])
            setattr(self, item, level_member)
            self.add_level_member_metadata(attribute)
            return level_member
        else:
            raise NotImplementedError("Level getattr failed. Item failed to be retrieved: ", item)

    def __repr__(self):
        return self.name


def get_db_cursor(engine):
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()
