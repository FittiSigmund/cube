from __future__ import annotations

import csv

import numpy as np
import pandas as pd

import engines
from session.session import *

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "ssb_snowflake"

postgres_engine: Postgres = engines.postgres(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT)
postgres = create_session(postgres_engine)
view = postgres.load_view('ssb_snowflake')

view2 = view.columns(view.date.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where((view.part.category.ca_category == "MFGR#12") &
                   (view.supplier.region.r_region == "AMERICA")) \
            .measures(view.lo_revenue, revenue=view.lo_extendedprice * view.lo_discount)
result = view2.output(1)

view3 = view.columns(view.part.mfgr.m_mfgr.members())\
            .rows(view.date.year.y_year.members())\
            .measures(view.lo_revenue, test=view.lo_extendedprice * view.lo_discount)
result = view3.output(1)

# df = pd.DataFrame(data={"AB": {"CD": 1, "EF": 2}, "AK": {"CD": 3, "EF": 4}}, dtype=object)
# df.at["CD", "AB"] = 5

hej = 1
# ## ssb query 2.1
# view.columns(view.date.year.y_year.members()) \
#     .rows(view.part.brand1.b_brand1.members()) \
#     .where(view.part.category.ca_category = "MFGR#12" &
#            view.supplier.region.r_region = "AMERICA") \
#     .measures(view.lo_revenue)


# print()
# print("Measures: ", cube.measures())
# print("Dimensions: ", cube.dimensions())
# print("Date hierarchy: ", cube.date.hierarchies())
# print("Supplier name dictionary: ", cube.supplier.supplier_name.__dict__)
# print("Date dimension dictionary: ", cube.date.__dict__)
# print("Date year level dictionary: ", cube.date.date_year.__dict__)
# print("2022 Level member: ", cube.date.date_year["2022"]["January"])

# print("Output of the cube (cube): ", cube.output())
# print("Date dimension current level: ", cube._dimension_list[1].current_level)
# c1 = cube._drill_down(cube.date)
# print("Date dimension current level: ", c1._dimension_list[1].current_level)
# print("c1 dictionary: ", c1.__dict__)
# print("Output of the cube (c1): ", c1.output())

# c2 = c1._drill_down(c1.date)
# print("Cube (c2) dictionary: ", c2.date.__dict__)
# c3 = c2._slice(c2.date, c2.date.date_year._2022._January)
# print(c3._dimension_list[1].__dict__)


# print(c2._dimension_list[1].current_level)
# c3 = c2._roll_up(c2.date)
# print(c3._dimension_list[1].current_level)
# c4 = c3._roll_up(c3.date)
# print(c4._dimension_list[1].current_level)


# print("Date dimension dictionary: ", cube.date.__dict__)
# cube.date._roll_up()
# print("Date dimension dictionary: ", cube.date.__dict__)

# print("2022 Level member dictionary: ", cube.date.date_year._2022.__dict__)
# print("January Level member dictionary: ", cube.date.date_year._2022._January.__dict__)
# print("Day 1 Level member dictionary: ", cube.date.date_year._2022._January._1.__dict__)

# ## Supplier testing
# print("Supplier hierarchy: ", cube.supplier.hierarchies())
# print("Nation dictionary: ", cube.supplier.supplier_nation.__dict__)
# print("Denmark dictionary: ", cube.supplier.supplier_nation._Denmark.__dict__)


# cube.columns([cube.date.date_year._2022._January])
# print("The output: ", cube.output())


# print()
# print(cube.store.store_address["Jyllandsgade 1"])
# print(cube.store.store_address["Jyllandsgade 2"])
#
# cube.where(cube.supplier.supplier_nation == "Denmark" and cube.store.store_address == "Jyllandsgade 1")



# conn = psycopg2.connect(
#     dbname="salesdb_snowflake_test",
#     user="sigmundur",
#     password=""
# )


# # Cube -> View syntactic suger not implemented
# view._axes = []
# view = view.axis(0, view.cube.date.date_month.members())


# output = view.output()
# print(output)
# with conn:
#     with conn.cursor() as curs:
#         curs.execute(output)
#         print(curs.fetchall())


def generate_data():
    supplier = 1221
    store = 4444
    product = 9012
    total_sales_price = np.random.randint(10, high=1000, size=721)
    unit_sales = np.random.randint(1, high=20, size=721)
    sale_date = [i for i in range(1, 722)]

    with open("/home/sigmundur/test/test.csv", "w") as out:
        csv_out = csv.writer(out)
        for i in range(len(sale_date)):
            csv_out.writerow((supplier, store, product, sale_date[i], total_sales_price[i], unit_sales[i]))
