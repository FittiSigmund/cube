import psycopg2
from psycopg2 import Error

from Cube import Level, Hierarchy, Dimension, Cube, Measure

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "salesdb_snowflake"


def init_cube():
    backend = "MONDRIAN"
    l1 = Level("Region", backend)
    l2 = Level("Nation", backend)
    l3 = Level("City", backend)
    h1 = Hierarchy([l1, l2, l3], backend)
    d1 = Dimension("Supplier", h1, [l1, l2, l3], backend)
    l1 = Level("Commune", backend)
    l2 = Level("Address", backend)
    h2 = Hierarchy([l1, l2], backend)
    d2 = Dimension("Customer", h2, [l1, l2, l3], backend)
    l1 = Level("Category", backend)
    l2 = Level("Type", backend)
    h3 = Hierarchy([l1, l2], backend)
    d3 = Dimension("Order", h3, [l1, l2, l3], backend)
    m1 = Measure("unit sales")
    return Cube([d1, d2, d3], [], [], backend)


def test_cube():
    cube = init_cube()
    cube2 = cube.columns(cube.Supplier.City.name)
    cube3 = cube.rows(cube.Customer.Address.name)
    cube4 = cube.columns(cube.Supplier.City.name).rows(cube.Customer.Address.name)
    print(cube2.__str__())
    print(cube3.__str__())
    print(cube4.__str__())


def get_cursor(user, password, host, port, database):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    return connection.cursor()


def infer_cube_structure(db_cursor):
    db_cursor.execute("""
        SELECT 
            table_info.table_schema,
            table_info.table_name,
            '>- no FKs' as foreign_keys
        FROM information_schema.tables AS table_info
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_info.table_type = 'BASE TABLE';
        """)

    return db_cursor.fetchall()


try:
    cursor = get_cursor(DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME)
    result = infer_cube_structure(cursor)
    print("Number of results: ", len(result))
    for i in range(0, len(result)):
        print("Table Schema -> ", result[i][0], "| Table Name -> ", result[i][1], "| foreign_keys -> ", result[i][2])
except (Exception, Error) as error:
    print("Error connecting to Postgres", error)
