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
print()
print("Measures: ", cube.measures())
print("Dimensions: ", cube.dimensions())
print("Date hierarchy: ", cube.date.hierarchies())
print("Date year level dictionary: ", cube.date.date_year.__dict__)
print("2022 Level member dictionary: ", cube.date.date_year._2022.__dict__)
print("January Level member dictionary: ", cube.date.date_year._2022._January.__dict__)
print("Day 1 Level member dictionary: ", cube.date.date_year._2022._January._1.__dict__)

### Supplier testing
# print("Supplier hierarchy: ", cube.supplier.hierarchies())
# print("Europe dictionary: ", cube.supplier.supplier_continent._Europe.__dict__)
# print("Denmark dictionary: ", cube.supplier.supplier_continent._Europe._Denmark.__dict__)
# print("POMPdeLUX dictionary: ", cube.supplier.supplier_continent._Europe._Denmark._POMPdeLUX.__dict__)


print(cube.__dict__)
cube.columns([cube.date.date_year._2022._January])
print(cube.output())
