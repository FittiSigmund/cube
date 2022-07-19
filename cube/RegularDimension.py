import psycopg2

from cube.Dimension import Dimension
from cube.TopLevel import TopLevel


def get_db_cursor(engine):
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()


class RegularDimension(Dimension):
    def __init__(self, name, level_list, engine, fact_table_fk):
        super().__init__(name, level_list)
        self._lowest_level = level_list[0]
        self.engine = engine
        self._cursor = get_db_cursor(engine)
        self._metadata = None
        self.fact_table_fk = fact_table_fk
        for level in level_list:
            level.dimension = self
            if not isinstance(level, TopLevel):
                setattr(self, level.name, level)
            else:
                self.current_level = level

    def lowest_level(self):
        return self._lowest_level

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, metadata):
        self._metadata = metadata
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
