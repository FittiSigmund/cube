import psycopg2

from rdflib import Namespace

from cube.Level import Level
from cube.LevelMember import LevelMember

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


class NonTopLevel(Level):
    def __init__(self, name, level_member_name, engine, pk, fk):
        super().__init__(name, parent=None, child=None, dimension=None)
        self._level_member_name = level_member_name
        self._pk_name = pk
        self._fk_name = fk
        self._engine = engine
        self._metadata = None
        self._level_members = []

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, value):
        self._child = value

    @property
    def pk_name(self):
        return self._pk_name

    @property
    def level_member_name(self):
        return self._level_member_name

    def members(self):
        raise NotImplementedError("Lazy version not implemented")

    def _fetch_attribute_from_db(self, attribute):
        conn = self._get_db_conn()
        with conn.cursor() as curs:
            curs.execute(f"""
                SELECT {self._level_member_name}
                FROM {self.name}
                WHERE {self._level_member_name} = '{attribute}';
            """)
            result = curs.fetchall()
        conn.close()
        return result

    def create_level_member(self, name):
        return LevelMember(name, self, self._metadata, self._engine)

    def _fetch_level_member_from_db_and_save_as_attribute(self, item):
        db_result = self._fetch_attribute_from_db(item)

        if db_result:
            level_member = self.create_level_member(db_result[0][0])
            setattr(self, item, level_member)
            self._level_members.append(level_member)
            return level_member
        else:
            raise AttributeError(f"'{self.name}' level does not contain the '{item}' level member")

    def __getattr__(self, item):
        return self._fetch_level_member_from_db_and_save_as_attribute(item)

    def __getitem__(self, item):
        result = next((x for x in self._level_members if x.name == item), False)
        if result:
            return result
        return self._fetch_level_member_from_db_and_save_as_attribute(item)

    # def __eq__(self, other):
    #     return lambda x: x.name == other

    def __repr__(self):
        return f"NonTopLevel({self.name})"

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)
