import engines
from session.session import *

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "salesdb_snowflake"

postgres_engine = engines.postgres(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT)
postgres = create_session(postgres_engine)
cube = postgres.load_cube('salesdb_snowflake')
print(cube.measures())
print(cube.dimensions())

