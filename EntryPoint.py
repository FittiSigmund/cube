import psycopg2
from psycopg2 import Error

from Cube import Level, Hierarchy, Dimension, Cube

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "salesdb_snowflake"


def get_cursor(user, password, host, port, database):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    return connection.cursor()


def get_table_with_no_fks(db_cursor):
    db_cursor.execute("""
        SELECT
            table_info.table_name
        FROM information_schema.tables AS table_info
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_info.table_type = 'BASE TABLE'
        AND table_info.table_schema || '.' || table_info.table_name NOT IN 
            (SELECT DISTINCT table_schema || '.' || table_name
             FROM information_schema.table_constraints
             WHERE constraint_type = 'FOREIGN KEY');
    """)

    return db_cursor.fetchall()


def get_next_levels(db_cursor, level):
    db_cursor.execute(f"""
        SELECT relname
        FROM pg_class
        WHERE pg_class.oid = (
            SELECT conrelid
            FROM pg_constraint
            WHERE pg_constraint.contype = 'f'
            AND pg_constraint.confrelid = (SELECT oid FROM pg_class WHERE pg_class.relname = '{level}')
        )
    """)
    return db_cursor.fetchall()


def get_levels(db_cursor, level):
    level_name_list = [level]
    current_level = level
    fact_table_found = False

    while fact_table_found is False:
        next_level = get_next_levels(db_cursor, current_level)[0][0]
        potential_level = get_next_levels(db_cursor, next_level)

        if potential_level:
            level_name_list.append(next_level)
            current_level = next_level
            continue
        else:
            fact_table_found = True

    return list(map(lambda x: Level(x), level_name_list))


def create_hierarchy(db_cursor, level_tuple):
    levels = get_levels(db_cursor, level_tuple[0])
    return Hierarchy(levels)


def create_dimension(hierarchy):
    dimension_name = hierarchy.levels[0].name.split('_')[0]
    return Dimension(dimension_name, hierarchy, hierarchy.levels)


def create_cube(dimension_list):
    return Cube(dimension_list)


def create_hierarchies(db_cursor, no_fk_table_list):
    return list(map(lambda x: create_hierarchy(db_cursor, x), no_fk_table_list))
    # hierarchy_list = []
    # for i in range(0, len(no_fk_table_list)):
    #     hierarchy_list.append(create_hierarchy(cursor, no_fk_table_list[i]))
    # return hierarchy_list


def create_dimensions(hierarchy_list):
    return list(map(lambda x: create_dimension(x), hierarchy_list))
    # dimension_list = []
    # for i in range(0, len(hierarchy_list)):
    #     dimension_list.append(create_dimension(hierarchy_list[i]))
    # return dimension_list


class Engines:
    def postgres(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port


## TODO: Implement sections of the running example
class Session:
    def create_session(self, engine):
        try:
            cursor = get_cursor(engine.user, engine.password, engine.host, engine.port, engine.dbname)
            table_with_no_fks = get_table_with_no_fks(cursor)
            hierarchies = create_hierarchies(cursor, table_with_no_fks)
            dimensions = create_dimensions(hierarchies)
            cube = create_cube(dimensions)
        except (Exception, Error) as error:
            print("ERROR: ", error)
