# This is where I will implement all ssb query flights in both plain pandas and in pyCube
# The script will be parameterisated so that another bash script can invoke til script with parameter which decides
# which use case to invoke from this script.
# The bash script will run this script using GNU time iteratively over every use case.
# The bash script will save the results of time into files for later analysis.
import sys
from typing import Dict, Callable

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import engines
from session.session import *

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "ssb_snowflake"

postgres_engine: Postgres = engines.postgres(DATABASE_NAME,
                                             DATABASE_USER,
                                             DATABASE_PASSWORD,
                                             DATABASE_HOST,
                                             DATABASE_PORT)

postgres = create_session(postgres_engine)
view = postgres.load_view('ssb_snowflake')

engine = create_engine("postgresql+psycopg2://sigmundur:@localhost/ssb_snowflake")

if sys.argv and len(sys.argv) == 2:
    # Call the right method
    pass


# Baseline1: Tag alle kolonner med og join fact tabellen først
# Baseline2: Tag kun nødvendige kolonner med og join fact tabellen sidst
# Baseline3: Lav alle joins i db
class Experiments:
    def pyCube_query11(self):
        view2 = view.where(
            (view.date1.year.y_year == 1993)
            & (view.lo_discount > 0)
            & (view.lo_discount < 4)
            & (view.lo_quantity < 25)) \
            .measures(revenue=view.lo_extendedprice * view.lo_discount)
        return view2.output(hack=True)

    def pandas_query11_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
            year_table = pd.read_sql("year", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        merged_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        filtered_table = merged_table[
            (merged_table["y_year"] == 1993)
            & (merged_table["lo_discount"] > 0)
            & (merged_table["lo_discount"] < 4)
            & (merged_table["lo_quantity"] < 25)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pandas_query11_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_discount", "lo_quantity", "lo_extendedprice"]
            )
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
        engine.dispose()
        temp1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        temp2 = temp1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        merged_table = fact_table.merge(temp2, left_on="lo_orderdate", right_on="d_datekey")
        # temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        # temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        # merged_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        filtered_table = merged_table[
            (merged_table["y_year"] == 1993)
            & (merged_table["lo_discount"] > 0)
            & (merged_table["lo_discount"] < 4)
            & (merged_table["lo_quantity"] < 25)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pyCube_query12(self):
        view2 = view.where(
            (view.date1.month.mo_yearmonthnum == 199401)
            & (view.lo_discount > 3)
            & (view.lo_discount < 7)
            & (view.lo_quantity > 25)
            & (view.lo_quantity < 36)
        ).measures(revenue=view.lo_extendedprice * view.lo_discount)
        return view2.output(hack=True)

    def pandas_query12_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        merged_table = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        filtered_table = merged_table[
            (merged_table["mo_yearmonthnum"] == 199401)
            & (merged_table["lo_discount"] > 3)
            & (merged_table["lo_discount"] < 7)
            & (merged_table["lo_quantity"] > 25)
            & (merged_table["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pandas_query12_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_discount", "lo_quantity", "lo_extendedprice"]
            )
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn)
        engine.dispose()
        temp1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        merged_table = fact_table.merge(temp1, left_on="lo_orderdate", right_on="d_datekey")
        filtered_table = merged_table[
            (merged_table["mo_yearmonthnum"] == 199401)
            & (merged_table["lo_discount"] > 3)
            & (merged_table["lo_discount"] < 7)
            & (merged_table["lo_quantity"] > 25)
            & (merged_table["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pyCube_query13(self):
        view2 = view.where(
            (view.date1.date.d_daynuminyear > 0)
            & (view.date1.date.d_daynuminyear < 8)
            & (view.date1.year.y_year == 1994)
            & (view.lo_discount > 4)
            & (view.lo_discount < 8)
            & (view.lo_quantity > 25)
            & (view.lo_quantity < 36)
        ).measures(revenue=view.lo_extendedprice * view.lo_discount)
        return view2.output(hack=True)

    def pandas_query13_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
            year_table = pd.read_sql("year", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        merged_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        filtered_table = merged_table[
            (merged_table["d_daynuminyear"] > 0)
            & (merged_table["d_daynuminyear"] < 8)
            & (merged_table["y_year"] == 1994)
            & (merged_table["lo_discount"] > 4)
            & (merged_table["lo_discount"] < 8)
            & (merged_table["lo_quantity"] > 25)
            & (merged_table["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pandas_query13_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_discount", "lo_quantity", "lo_extendedprice"]
            )
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey", "d_daynuminyear"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        merged_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        filtered_table = merged_table[
            (merged_table["d_daynuminyear"] > 0)
            & (merged_table["d_daynuminyear"] < 8)
            & (merged_table["y_year"] == 1994)
            & (merged_table["lo_discount"] > 4)
            & (merged_table["lo_discount"] < 8)
            & (merged_table["lo_quantity"] > 25)
            & (merged_table["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])

    def pyCube_query21(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.category.ca_category == "MFGR#12")
            & (view.supplier.region.r_region == "AMERICA")
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query21_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn)
            brand_table = pd.read_sql("brand1", conn)
            category_table = pd.read_sql("category", conn)
            supplier_table = pd.read_sql("supplier", conn)
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        temp2 = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        fact_cat_table = temp2.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        temp1 = fact_cat_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        merged_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        filtered_table = merged_table[
            (merged_table["ca_category"] == "MFGR#12") & (merged_table["r_region"] == "AMERICA     ")]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pandas_query21_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_partkey", "lo_suppkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn)
            category_table = pd.read_sql("category", conn, columns=["ca_categorykey", "ca_category"])
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn, columns=["n_nationkey", "n_regionkey"])
            region_table = pd.read_sql("region", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        part2 = part1.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")

        supplier1 = supplier_table.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        supplier2 = supplier1.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supplier3 = supplier2.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part2, left_on="lo_partkey", right_on="p_partkey")
        merged_table = merged_table2.merge(supplier3, left_on="lo_suppkey", right_on="s_suppkey")

        filtered_table = merged_table[
            (merged_table["ca_category"] == "MFGR#12") & (merged_table["r_region"] == "AMERICA     ")]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pyCube_query22(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.brand1.b_brand1 > "MFGR#2220")
            & (view.part.brand1.b_brand1 < "MFGR#2229")
            & (view.supplier.region.r_region == "ASIA")
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query22_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn)
            brand_table = pd.read_sql("brand1", conn)
            supplier_table = pd.read_sql("supplier", conn)
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        fact_cat_table = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        temp1 = fact_cat_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        merged_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        filtered_table = merged_table[
            (merged_table["b_brand1"] >= "MFGR#2221")
            & (merged_table["b_brand1"] <= "MFGR#2228")
            & (merged_table["r_region"] == "ASIA        ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pandas_query22_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_partkey", "lo_suppkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn, columns=["n_nationkey", "n_regionkey"])
            region_table = pd.read_sql("region", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")

        supplier1 = supplier_table.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        supplier2 = supplier1.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supplier3 = supplier2.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part1, left_on="lo_partkey", right_on="p_partkey")
        merged_table = merged_table2.merge(supplier3, left_on="lo_suppkey", right_on="s_suppkey")

        filtered_table = merged_table[
            (merged_table["b_brand1"] >= "MFGR#2221")
            & (merged_table["b_brand1"] <= "MFGR#2228")
            & (merged_table["r_region"] == "ASIA        ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pyCube_query23(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.brand1.b_brand1 == "MFGR#2339")
            & (view.supplier.region.r_region == "EUROPE")
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query23_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn)
            date_table = pd.read_sql("date", conn)
            month_table = pd.read_sql("month", conn)
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn)
            brand_table = pd.read_sql("brand1", conn)
            supplier_table = pd.read_sql("supplier", conn)
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        fact_cat_table = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        temp1 = fact_cat_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        merged_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        filtered_table = merged_table[
            (merged_table["b_brand1"] == "MFGR#2339")
            & (merged_table["r_region"] == "EUROPE      ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pandas_query23_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_partkey", "lo_suppkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn, columns=["n_nationkey", "n_regionkey"])
            region_table = pd.read_sql("region", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")

        supplier1 = supplier_table.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        supplier2 = supplier1.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supplier3 = supplier2.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part1, left_on="lo_partkey", right_on="p_partkey")
        merged_table = merged_table2.merge(supplier3, left_on="lo_suppkey", right_on="s_suppkey")

        filtered_table = merged_table[
            (merged_table["b_brand1"] == "MFGR#2339")
            & (merged_table["r_region"] == "EUROPE      ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)

    def pyCube_query31(self):
        view2 = view.columns(view.customer.nation.n_nation.members()) \
            .rows(view.supplier.nation.n_nation.members()) \
            .pages(view.date1.year.y_year.members()) \
            .where(
            (view.customer.region.r_region == "ASIA")
            & (view.supplier.region.r_region == "ASIA")
            & (view.date1.year.y_year >= 1992)
            & (view.date1.year.y_year <= 1997)
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query31_baseline1(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn,
                                     columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        fact_supp_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        temp1 = fact_supp_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        temp2 = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey", suffixes=(None, "_c"))
        merged_table = temp3.merge(region_table, left_on="n_regionkey_c", right_on="r_regionkey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (merged_table["r_region_c"] == "ASIA        ")
            & (merged_table["r_region"] == "ASIA        ")
            & (merged_table["y_year"] >= 1992)
            & (merged_table["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="n_nation",
            columns=["n_nation_c", "y_year"],
            aggfunc=np.sum
        )

    def pandas_query31_baseline2(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        supp_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supp_geo2 = supp_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        cust_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        cust_geo2 = cust_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        supp = supplier_table.merge(supp_geo2, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(cust_geo2, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table2.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

        filtered_table = merged_table[
            (merged_table["r_region_c"] == "ASIA        ")
            & (merged_table["r_region"] == "ASIA        ")
            & (merged_table["y_year"] >= 1992)
            & (merged_table["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="n_nation",
            columns=["n_nation_c", "y_year"],
            aggfunc=np.sum
        )

    def pyCube_query32(self):
        view2 = view.columns(view.customer.city.ci_city.members()) \
            .rows(view.supplier.city.ci_city.members()) \
            .pages(view.date1.year.y_year.members()) \
            .where(
            (view.customer.nation.n_nation == "UNITED STATES")
            & (view.supplier.nation.n_nation == "UNITED STATES")
            & (view.date1.year.y_year >= 1992)
            & (view.date1.year.y_year <= 1997)
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query32(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn,
                                     columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        fact_supp_table = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        temp1 = fact_supp_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        temp2 = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        merged_table = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey",
                                   suffixes=(None, "_c"))
        filtered_table = merged_table[
            (merged_table["n_nation_c"] == "UNITED STATES  ")
            & (merged_table["n_nation"] == "UNITED STATES  ")
            & (merged_table["y_year"] >= 1992)
            & (merged_table["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )

    def pyCube_query33(self):
        view2 = view.columns(view.customer.city.ci_city.members()) \
            .rows(view.supplier.city.ci_city.members()) \
            .pages(view.date1.year.y_year.members()) \
            .where(
            ((view.customer.city.ci_city == "UNITED KI1")
             | (view.customer.city.ci_city == "UNITED KI5"))
            & ((view.supplier.city.ci_city == "UNITED KI1")
               | (view.supplier.city.ci_city == "UNITED KI5"))
            & (view.date1.year.y_year >= 1992)
            & (view.date1.year.y_year <= 1997)
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query33(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn,
                                     columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        fact_supp_table = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp1 = fact_supp_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        merged_table = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (
                    (merged_table["ci_city_c"] == "UNITED KI1")
                    | (merged_table["ci_city_c"] == "UNITED KI5")
            )
            & (
                    (merged_table["ci_city"] == "UNITED KI1")
                    | (merged_table["ci_city"] == "UNITED KI5")
            )
            & (merged_table["y_year"] >= 1992)
            & (merged_table["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )

    def pyCube_query34(self):
        view2 = view.columns(view.customer.city.ci_city.members()) \
            .rows(view.supplier.city.ci_city.members()) \
            .pages(view.date1.year.y_year.members()) \
            .where(
            ((view.customer.city.ci_city == "UNITED KI1")
             | (view.customer.city.ci_city == "UNITED KI5"))
            & ((view.supplier.city.ci_city == "UNITED KI1")
               | (view.supplier.city.ci_city == "UNITED KI5"))
            & (view.date1.month.mo_yearmonth == "Dec1997")
        ) \
            .measures(view.lo_revenue)
        return view2.output()

    def pandas_query34(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn,
                                     columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearmonth", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        fact_supp_table = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp1 = fact_supp_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        merged_table = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (
                    (merged_table["ci_city_c"] == "UNITED KI1")
                    | (merged_table["ci_city_c"] == "UNITED KI5")
            )
            & (
                    (merged_table["ci_city"] == "UNITED KI1")
                    | (merged_table["ci_city"] == "UNITED KI5")
            )
            & (merged_table["mo_yearmonth"] == "Dec1997")
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )

    def pyCube_query41(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.customer.nation.n_nation.members()) \
            .where(
            (view.customer.region.r_region == "AMERICA")
            & (view.supplier.region.r_region == "AMERICA")
            & (
                    (view.part.mfgr.m_mfgr == "MFGR#1")
                    | (view.part.mfgr.m_mfgr == "MFGR#2")
            )
        ) \
            .measures(profit=view.lo_revenue - view.lo_supplycost)
        return view2.output()

    def pandas_query41(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn, columns=[
                "lo_orderdate",
                "lo_suppkey",
                "lo_custkey",
                "lo_partkey",
                "lo_revenue",
                "lo_supplycost"
            ])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn, columns=["b_brand1key", "b_categorykey"])
            category_table = pd.read_sql("category", conn)
            mfgr_table = pd.read_sql("mfgr", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        fact_supp_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        temp1 = fact_supp_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        temp2 = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        temp3 = temp2.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        fact_part_table = temp3.merge(mfgr_table, left_on="ca_mfgrkey", right_on="m_mfgrkey")
        temp1 = fact_part_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        temp2 = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey", suffixes=(None, "_c"))
        merged_table = temp3.merge(region_table, left_on="n_regionkey_c", right_on="r_regionkey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (merged_table["r_region_c"] == "AMERICA     ")
            & (merged_table["r_region"] == "AMERICA     ")
            & (
                    (merged_table["m_mfgr"] == "MFGR#1")
                    | (merged_table["m_mfgr"] == "MFGR#2")
            )
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="n_nation_c",
            columns="y_year",
            aggfunc=np.sum
        )

    def pyCube_query42(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.supplier.nation.n_nation.members()) \
            .pages(view.part.category.ca_category.members()) \
            .where(
            (view.customer.region.r_region == "AMERICA")
            & (view.supplier.region.r_region == "AMERICA")
            & (
                    (view.date1.year.y_year == 1997)
                    | (view.date1.year.y_year == 1998)
            )
            & (
                    (view.part.mfgr.m_mfgr == "MFGR#1")
                    | (view.part.mfgr.m_mfgr == "MFGR#2")
            )
        ) \
            .measures(profit=view.lo_revenue - view.lo_supplycost)
        return view2.output()

    def pandas_query42(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn, columns=[
                "lo_orderdate",
                "lo_suppkey",
                "lo_custkey",
                "lo_partkey",
                "lo_revenue",
                "lo_supplycost"
            ])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn, columns=["b_brand1key", "b_categorykey"])
            category_table = pd.read_sql("category", conn)
            mfgr_table = pd.read_sql("mfgr", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        fact_supp_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        temp1 = fact_supp_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        temp2 = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        temp3 = temp2.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        fact_part_table = temp3.merge(mfgr_table, left_on="ca_mfgrkey", right_on="m_mfgrkey")
        temp1 = fact_part_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        temp2 = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey", suffixes=(None, "_c"))
        merged_table = temp3.merge(region_table, left_on="n_regionkey_c", right_on="r_regionkey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (merged_table["r_region_c"] == "AMERICA     ")
            & (merged_table["r_region"] == "AMERICA     ")
            & (
                    (merged_table["y_year"] == 1997)
                    | (merged_table["y_year"] == 1998)
            )
            & (
                    (merged_table["m_mfgr"] == "MFGR#1")
                    | (merged_table["m_mfgr"] == "MFGR#2")
            )
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="n_nation",
            columns=["y_year", "ca_category"],
            aggfunc=np.sum
        )

    def pyCube_query43(self):
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.supplier.city.ci_city.members()) \
            .pages(view.part.brand1.b_brand1.members()) \
            .where(
            (view.customer.region.r_region == "AMERICA")
            & (view.supplier.nation.n_nation == "UNITED STATES")
            & (
                    (view.date1.year.y_year == 1997)
                    | (view.date1.year.y_year == 1998)
            )
            & (view.part.category.ca_category == "MFGR#14")
        ) \
            .measures(profit=view.lo_revenue - view.lo_supplycost)
        return view2.output()

    def pandas_query43(self):
        with engine.connect() as conn:
            fact_table = pd.read_sql("lineorder", conn, columns=[
                "lo_orderdate",
                "lo_suppkey",
                "lo_custkey",
                "lo_partkey",
                "lo_revenue",
                "lo_supplycost"
            ])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey"])
            year_table = pd.read_sql("year", conn)
            part_table = pd.read_sql("part", conn, columns=["p_partkey", "p_brand1key"])
            brand_table = pd.read_sql("brand1", conn)
            category_table = pd.read_sql("category", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)
        engine.dispose()
        temp1 = fact_table.merge(date_table, left_on="lo_orderdate", right_on="d_datekey")
        temp2 = temp1.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        fact_date_table = temp2.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")
        temp1 = fact_date_table.merge(supplier_table, left_on="lo_suppkey", right_on="s_suppkey")
        temp2 = temp1.merge(city_table, left_on="s_citykey", right_on="ci_citykey")
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        fact_supp_table = temp3.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")
        temp1 = fact_supp_table.merge(part_table, left_on="lo_partkey", right_on="p_partkey")
        temp2 = temp1.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        fact_part_table = temp2.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        temp1 = fact_part_table.merge(customer_table, left_on="lo_custkey", right_on="c_custkey")
        temp2 = temp1.merge(city_table, left_on="c_citykey", right_on="ci_citykey", suffixes=(None, "_c"))
        temp3 = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey", suffixes=(None, "_c"))
        merged_table = temp3.merge(region_table, left_on="n_regionkey_c", right_on="r_regionkey", suffixes=(None, "_c"))
        filtered_table = merged_table[
            (merged_table["r_region_c"] == "AMERICA     ")
            & (merged_table["n_nation"] == "UNITED STATES  ")
            & (
                    (merged_table["y_year"] == 1997)
                    | (merged_table["y_year"] == 1998)
            )
            & (merged_table["ca_category"] == "MFGR#14")
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="ci_city",
            columns=["y_year", "b_brand1"],
            aggfunc=np.sum
        )

    # Create new view before calling query methods
    def compare(
            self,
            baseline: int = None,
            single: bool = False,
            pyCube_method: Callable[[], pd.DataFrame] = None,
            pandas_method: Callable[[], pd.DataFrame] = None,
            first_query_flight: bool = False) -> None:
        def prepare_dfs(pyCube_df: pd.DataFrame, pandas_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
            pyCube_df.columns = pyCube_result.columns.droplevel(level=pyCube_result.columns.nlevels - 1)
            pyCube_df.sort_index(inplace=True, axis=0)
            pyCube_df.sort_index(inplace=True, axis=1)
            pandas_df.sort_index(inplace=True, axis=0)
            pandas_df.sort_index(inplace=True, axis=1)
            return pyCube_df, pandas_df

        if single:
            pyCube_result = pyCube_method()
            pandas_result = pandas_method()
            if not first_query_flight:
                pyCube_result, pandas_result = prepare_dfs(pyCube_result, pandas_result)
            print(pyCube_result.equals(pandas_result))
            return

        query_numbers: Dict[int, int] = {1: 3, 2: 3, 3: 4, 4: 3}
        global view
        for k, v in query_numbers.items():
            if k == 1:
                for i in range(1, v + 1):
                    pyCube_result = self.__getattribute__(f"pyCube_query{k}{i}")()
                    pandas_result = self.__getattribute__(f"pandas_query{k}{i}")()
                    print(pyCube_result.equals(pandas_result))
            else:
                for i in range(1, v + 1):
                    postgres = create_session(postgres_engine)
                    view = postgres.load_view('ssb_snowflake')
                    pyCube_result, pandas_result = prepare_dfs(
                        self.__getattribute__(f"pyCube_query{k}{i}_baseline{baseline}")(),
                        self.__getattribute__(f"pandas_query{k}{i}_baseline{baseline}")())
                    print(pyCube_result.equals(pandas_result))


# pyCube_result = pyCube_query41()
# pandas_result = pandas_query41()
# pyCube_result.columns = pyCube_result.columns.droplevel(level=2)
# result = pyCube_result.equals(pandas_result)
# hej = 1

## KOMIN TIL PANDAS_QUERY32_BASELINE2 (SUM IKKI ER TIL ENN)
e = Experiments()
e.compare(
    baseline=2,
    single=True,
    pyCube_method=e.pyCube_query31,
    pandas_method=e.pandas_query31_baseline2,
    first_query_flight=False)
