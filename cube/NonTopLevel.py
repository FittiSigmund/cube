import psycopg2

from rdflib import Namespace

from cube.Level import Level
from cube.LevelMember import LevelMember
from engines import Postgres

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def remove_underscore_prefix(item):
    return item[1::]


class NonTopLevel(Level):
    def __init__(self, name: str, level_member_name: str, engine: Postgres, pk: str, fk: str):
        super().__init__(name, parent=None, child=None, dimension=None)
        self._level_member_name: str = level_member_name
        self._pk_name: str = pk
        self._fk_name: str = fk
        self._engine: Postgres = engine
        self._metadata = None
        self._level_members = []
        self._all_lm_loaded: bool = False

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
    def fk_name(self):
        return self._fk_name

    @property
    def level_member_name(self):
        return self._level_member_name

    @property
    def all_lm_loaded(self) -> bool:
        return self._all_lm_loaded

    def members(self):
        if self._all_lm_loaded:
            return self._level_members
        else:
            db_result = self._fetch_all_lm_from_db()
            lms = []
            if db_result:
                for result in db_result:
                    lm_name = result[0]
                    lm = self.create_level_member(lm_name)
                    lms.append(lm)
                    setattr(self, str(lm_name), lm)
                    self._append_level_members_without_duplicates(lm)
                self._all_lm_loaded = True
                return lms
            else:
                raise AttributeError(f"'{self.name}' level does not contain any members")

    def _append_level_members_without_duplicates(self, lm):
        lms_str_list = list(map(lambda x: x.name, self._level_members))
        self._level_members.append(lm) if lm.name not in lms_str_list else None

    def _fetch_all_lm_from_db(self):
        conn = self._get_db_conn()
        with conn.cursor() as curs:
            curs.execute(f"SELECT {self._level_member_name} FROM {self.name};")
            result = curs.fetchall()
        conn.close()
        return result

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
            setattr(self, str(item), level_member)
            self._level_members.append(level_member)
            return level_member
        else:
            raise AttributeError(f"'{self.name}' level does not contain the '{item}' level member")

    def __getattr__(self, item):
        if item in self.__dict__.keys():
            return self.__dict__[item]
        else:
            return self._fetch_level_member_from_db_and_save_as_attribute(item)

    def __getitem__(self, item):
        if type(item) is int:
            item = int(item)
            result = next((x for x in self._level_members if x.name == item), False)
        else:
            result = next((x for x in self._level_members if x.name == item), False)
        if result:
            return result
        return self._fetch_level_member_from_db_and_save_as_attribute(item)

    def __eq__(self, other):
        def eq():
            this_level = self
            requirement = other
            print(this_level.name)
            print(requirement)
        return self.name

    def __repr__(self):
        return f"NonTopLevel({self.name})"

    def _get_db_conn(self):
        return psycopg2.connect(user=self._engine.user,
                                password=self._engine.password,
                                host=self._engine.host,
                                port=self._engine.port,
                                database=self._engine.dbname)
