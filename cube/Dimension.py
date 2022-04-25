import psycopg2


def get_db_cursor(engine):
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()


class Dimension:
    def __init__(self, name, level_list, engine):
        self._name = name
        self._lowest_level = level_list[0]
        self._cursor = get_db_cursor(engine)
        self._metadata = None
        self._level_list = level_list
        for level in level_list:
            setattr(self, level.name, level)

    @property
    def name(self):
        return self._name

    @property
    def lowest_level(self):
        return self._lowest_level

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, metadata):
        self._metadata = metadata
        for level in self._level_list:
            level._metadata = metadata

    def hierarchies(self):
        current_level = self.lowest_level
        hierarchy = [current_level]
        while current_level != current_level.parent:
            current_level = current_level.parent
            hierarchy.append(current_level)
        return hierarchy

    def __repr__(self):
        return self.name
