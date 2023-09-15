# This is where I will implement all ssb query flights in both plain pandas and in pyCube
# The script will be parameterisated so that another bash script can invoke til script with parameter which decides
# which use case to invoke from this script.
# The bash script will run this script using GNU time iteratively over every use case.
# The bash script will save the results of time into files for later analysis.
import sys
import time
from typing import Dict, Callable

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from session.session import *
import engines
from timers import PythonTimer, DBTimer

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

pd.options.mode.chained_assignment = None


# Baseline1: Tag alle kolonner med og join fact tabellen først
# Baseline2: Tag kun nødvendige kolonner med og join fact tabellen sidst
# Baseline3: Lav alle joins i db
def pyCube_query11():
    with PythonTimer():
        view2 = view.where(
            (view.date1.year.y_year == 1993)
            & (view.lo_discount > 0)
            & (view.lo_discount < 4)
            & (view.lo_quantity < 25)) \
            .measures(revenue=view.lo_extendedprice * view.lo_discount)
        return view2.output(hack=True)


def pandas_query11_baseline1():
    with PythonTimer():
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


def pandas_query11_baseline2():
    with PythonTimer():
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
        filtered_table = merged_table[
            (merged_table["y_year"] == 1993)
            & (merged_table["lo_discount"] > 0)
            & (merged_table["lo_discount"] < 4)
            & (merged_table["lo_quantity"] < 25)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])


def pandas_query11_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT lo_extendedprice, lo_discount, lo_quantity, y_year
                FROM lineorder JOIN date ON lo_orderdate = d_datekey 
                JOIN month ON d_monthkey = mo_monthkey 
                JOIN year ON mo_yearkey = y_yearkey
                """, conn)
        engine.dispose()

        filtered_table = df[
            (df["y_year"] == 1993)
            & (df["lo_discount"] > 0)
            & (df["lo_discount"] < 4)
            & (df["lo_quantity"] < 25)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])


def pyCube_query12():
    with PythonTimer():
        view2 = view.where(
            (view.date1.month.mo_yearmonthnum == 199401)
            & (view.lo_discount > 3)
            & (view.lo_discount < 7)
            & (view.lo_quantity > 25)
            & (view.lo_quantity < 36)
        ).measures(revenue=view.lo_extendedprice * view.lo_discount)
        return view2.output(hack=True)


def pandas_query12_baseline1():
    with PythonTimer():
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


def pandas_query12_baseline2():
    with PythonTimer():
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


def pandas_query12_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                 SELECT lo_extendedprice, lo_discount, lo_quantity, mo_yearmonthnum
                 FROM lineorder JOIN date ON lo_orderdate = d_datekey 
                 JOIN month ON d_monthkey = mo_monthkey 
                 """, conn)
        engine.dispose()

        filtered_table = df[
            (df["mo_yearmonthnum"] == 199401)
            & (df["lo_discount"] > 3)
            & (df["lo_discount"] < 7)
            & (df["lo_quantity"] > 25)
            & (df["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])


def pyCube_query13():
    with PythonTimer():
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


def pandas_query13_baseline1():
    with PythonTimer():
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


def pandas_query13_baseline2():
    with PythonTimer():
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


def pandas_query13_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_extendedprice, lo_discount, lo_quantity, y_year, d_daynuminyear
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
            """,
                             conn,
                             )
        engine.dispose()
        filtered_table = df[
            (df["d_daynuminyear"] > 0)
            & (df["d_daynuminyear"] < 8)
            & (df["y_year"] == 1994)
            & (df["lo_discount"] > 4)
            & (df["lo_discount"] < 8)
            & (df["lo_quantity"] > 25)
            & (df["lo_quantity"] < 36)
            ]
        return pd.DataFrame([filtered_table.apply(lambda x: x["lo_extendedprice"] * x["lo_discount"], axis=1).sum()],
                            columns=["revenue"])


def pyCube_query21():
    with PythonTimer():
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.category.ca_category == "MFGR#12")
            & (view.supplier.region.r_region == "AMERICA")
        ) \
            .measures(view.lo_revenue)
        return view2.output()


def pandas_query21_baseline1():
    with PythonTimer():
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


def pandas_query21_baseline2():
    with PythonTimer():
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


def pandas_query21_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue, ca_category, r_region, b_brand1, y_year
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN part ON lo_partkey = p_partkey 
                    JOIN brand1 ON p_brand1key = b_brand1key 
                    JOIN category ON b_categorykey = ca_categorykey 
                    JOIN supplier ON lo_suppkey = s_suppkey 
                    JOIN city ON s_citykey = ci_citykey 
                    JOIN nation ON ci_nationkey = n_nationkey 
                    JOIN region ON n_regionkey = r_regionkey 
                """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (df["ca_category"] == "MFGR#12")
            & (df["r_region"] == "AMERICA     ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)


def pyCube_query22():
    with PythonTimer():
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.brand1.b_brand1 > "MFGR#2220")
            & (view.part.brand1.b_brand1 < "MFGR#2229")
            & (view.supplier.region.r_region == "ASIA")
        ) \
            .measures(view.lo_revenue)
        return view2.output()


def pandas_query22_baseline1():
    with PythonTimer():
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


def pandas_query22_baseline2():
    with PythonTimer():
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


def pandas_query22_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue, b_brand1, r_region, y_year
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN part ON lo_partkey = p_partkey 
                    JOIN brand1 ON p_brand1key = b_brand1key 
                    JOIN supplier ON lo_suppkey = s_suppkey 
                    JOIN city ON s_citykey = ci_citykey 
                    JOIN nation ON ci_nationkey = n_nationkey 
                    JOIN region ON n_regionkey = r_regionkey 
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (df["b_brand1"] >= "MFGR#2221")
            & (df["b_brand1"] <= "MFGR#2228")
            & (df["r_region"] == "ASIA        ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)


def pyCube_query23():
    with PythonTimer():
        view2 = view.columns(view.date1.year.y_year.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .where(
            (view.part.brand1.b_brand1 == "MFGR#2339")
            & (view.supplier.region.r_region == "EUROPE")
        ) \
            .measures(view.lo_revenue)
        test = view2.output()
        return test


def pandas_query23_baseline1():
    with PythonTimer():
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


def pandas_query23_baseline2():
    with PythonTimer():
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


def pandas_query23_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue, b_brand1, r_region, y_year
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN part ON lo_partkey = p_partkey 
                    JOIN brand1 ON p_brand1key = b_brand1key 
                    JOIN supplier ON lo_suppkey = s_suppkey 
                    JOIN city ON s_citykey = ci_citykey 
                    JOIN nation ON ci_nationkey = n_nationkey 
                    JOIN region ON n_regionkey = r_regionkey 
            
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (df["b_brand1"] == "MFGR#2339")
            & (df["r_region"] == "EUROPE      ")
            ]
        return filtered_table.pivot_table(values="lo_revenue", index="b_brand1", columns="y_year", aggfunc=np.sum)


def pyCube_query31():
    with PythonTimer():
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


def pandas_query31_baseline1():
    with PythonTimer():
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


def pandas_query31_baseline2():
    with PythonTimer():
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


def pandas_query31_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue, 
                    y_year,
                    nation0.n_nation AS n_nation_c, 
                    region0.r_region AS r_region_c,
                    nation1.n_nation AS n_nation,
                    region1.r_region AS r_region
                FROM lineorder 
                    JOIN customer ON lo_custkey = c_custkey 
                    JOIN city AS city0 ON c_citykey = city0.ci_citykey 
                    JOIN nation AS nation0 ON city0.ci_nationkey = nation0.n_nationkey 
                    JOIN region AS region0 ON nation0.n_regionkey = region0.r_regionkey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN nation AS nation1 ON city1.ci_nationkey = nation1.n_nationkey 
                    JOIN region AS region1 ON nation1.n_regionkey = region1.r_regionkey 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (df["r_region_c"] == "ASIA        ")
            & (df["r_region"] == "ASIA        ")
            & (df["y_year"] >= 1992)
            & (df["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="n_nation",
            columns=["n_nation_c", "y_year"],
            aggfunc=np.sum
        )


def pyCube_query32():
    with PythonTimer():
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


def pandas_query32_baseline1():
    with PythonTimer():
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


def pandas_query32_baseline2():
    with PythonTimer():
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
            city_table = pd.read_sql("city", conn)
            nation_table = pd.read_sql("nation", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        supp_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")

        cust_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")

        supp = supplier_table.merge(supp_geo1, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(cust_geo1, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table2.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

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


def pandas_query32_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    city0.ci_city AS ci_city_c, 
                    nation0.n_nation AS n_nation_c,
                    city1.ci_city AS ci_city, 
                    nation1.n_nation AS n_nation,
                    y_year
                FROM lineorder 
                    JOIN customer AS customer0 ON lineorder.lo_custkey = customer0.c_custkey 
                    JOIN city AS city0 ON customer0.c_citykey = city0.ci_citykey 
                    JOIN nation AS nation0 ON city0.ci_nationkey = nation0.n_nationkey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN nation AS nation1 ON city1.ci_nationkey = nation1.n_nationkey 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (df["n_nation_c"] == "UNITED STATES  ")
            & (df["n_nation"] == "UNITED STATES  ")
            & (df["y_year"] >= 1992)
            & (df["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )


def pyCube_query33():
    with PythonTimer():
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


def pandas_query33_baseline1():
    with PythonTimer():
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


def pandas_query33_baseline2():
    with PythonTimer():
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
            city_table = pd.read_sql("city", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        supp = supplier_table.merge(city_table, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(city_table, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table2.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

        filtered_table = merged_table[
            (
                    (merged_table["ci_city_c"] == "UNITED KI1")
                    | (merged_table["ci_city_c"] == "UNITED KI5")
            ) &
            (
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


def pandas_query33_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    city0.ci_city AS ci_city_c, 
                    city1.ci_city AS ci_city, 
                    y_year 
                FROM lineorder 
                    JOIN customer AS customer0 ON lineorder.lo_custkey = customer0.c_custkey 
                    JOIN city AS city0 ON customer0.c_citykey = city0.ci_citykey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (
                    (df["ci_city_c"] == "UNITED KI1")
                    | (df["ci_city_c"] == "UNITED KI5")
            ) &
            (
                    (df["ci_city"] == "UNITED KI1")
                    | (df["ci_city"] == "UNITED KI5")
            )
            & (df["y_year"] >= 1992)
            & (df["y_year"] <= 1997)
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )


def pyCube_query34():
    with PythonTimer():
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


def pandas_query34_baseline1():
    with PythonTimer():
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


def pandas_query34_baseline2():
    with PythonTimer():
        with engine.connect() as conn:
            fact_table = pd.read_sql(
                "lineorder",
                conn,
                columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
            date_table = pd.read_sql("date", conn, columns=["d_datekey", "d_monthkey"])
            month_table = pd.read_sql("month", conn, columns=["mo_monthkey", "mo_yearkey", "mo_yearmonth"])
            year_table = pd.read_sql("year", conn)
            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])
            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])
            city_table = pd.read_sql("city", conn)
        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        supp = supplier_table.merge(city_table, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(city_table, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table2.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

        filtered_table = merged_table[
            (
                    (merged_table["ci_city_c"] == "UNITED KI1")
                    | (merged_table["ci_city_c"] == "UNITED KI5")
            ) &
            (
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


def pandas_query34_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    city0.ci_city AS ci_city_c, 
                    city1.ci_city AS ci_city, 
                    mo_yearmonth,
                    y_year 
                FROM lineorder 
                    JOIN customer AS customer0 ON lineorder.lo_custkey = customer0.c_custkey 
                    JOIN city AS city0 ON customer0.c_citykey = city0.ci_citykey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
            """,
                             conn)
        engine.dispose()

        filtered_table = df[
            (
                    (df["ci_city_c"] == "UNITED KI1")
                    | (df["ci_city_c"] == "UNITED KI5")
            ) &
            (
                    (df["ci_city"] == "UNITED KI1")
                    | (df["ci_city"] == "UNITED KI5")
            )
            & (df["mo_yearmonth"] == "Dec1997")
            ]
        return filtered_table.pivot_table(
            values="lo_revenue",
            index="ci_city",
            columns=["ci_city_c", "y_year"],
            aggfunc=np.sum
        )


def pyCube_query41():
    with PythonTimer():
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


def pandas_query41_baseline1():
    with PythonTimer():
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


def pandas_query41_baseline2():
    with PythonTimer():
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
            category_table = pd.read_sql("category", conn, columns=["ca_categorykey", "ca_mfgrkey"])
            mfgr_table = pd.read_sql("mfgr", conn)

            supplier_table = pd.read_sql("supplier", conn, columns=["s_suppkey", "s_citykey"])

            customer_table = pd.read_sql("customer", conn, columns=["c_custkey", "c_citykey"])

            city_table = pd.read_sql("city", conn, columns=["ci_citykey", "ci_nationkey"])
            nation_table = pd.read_sql("nation", conn)
            region_table = pd.read_sql("region", conn)

        engine.dispose()

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        part2 = part1.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        part3 = part2.merge(mfgr_table, left_on="ca_mfgrkey", right_on="m_mfgrkey")

        supp_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supp_geo2 = supp_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        cust_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        cust_geo2 = cust_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        supp = supplier_table.merge(supp_geo2, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(cust_geo2, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part3, left_on="lo_partkey", right_on="p_partkey")
        merged_table3 = merged_table2.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table3.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

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


def pandas_query41_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    lo_supplycost,
                    nation1.n_nation AS n_nation_c,
                    region1.r_region AS r_region_c,
                    region2.r_region AS r_region,
                    y_year,
                    m_mfgr
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN customer AS customer1 ON lineorder.lo_custkey = customer1.c_custkey 
                    JOIN city AS city1 ON customer1.c_citykey = city1.ci_citykey 
                    JOIN nation AS nation1 ON city1.ci_nationkey = nation1.n_nationkey 
                    JOIN region AS region1 ON nation1.n_regionkey = region1.r_regionkey 
                    JOIN supplier AS supplier2 ON lineorder.lo_suppkey = supplier2.s_suppkey 
                    JOIN city AS city2 ON supplier2.s_citykey = city2.ci_citykey 
                    JOIN nation AS nation2 ON city2.ci_nationkey = nation2.n_nationkey 
                    JOIN region AS region2 ON nation2.n_regionkey = region2.r_regionkey 
                    JOIN part ON lo_partkey = p_partkey 
                    JOIN brand1 ON p_brand1key = b_brand1key 
                    JOIN category ON b_categorykey = ca_categorykey 
                    JOIN mfgr ON ca_mfgrkey = m_mfgrkey 
                """,
                             conn
                             )
        engine.dispose()

        filtered_table = df[
            (df["r_region_c"] == "AMERICA     ")
            & (df["r_region"] == "AMERICA     ")
            & (
                    (df["m_mfgr"] == "MFGR#1")
                    | (df["m_mfgr"] == "MFGR#2")
            )
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="n_nation_c",
            columns="y_year",
            aggfunc=np.sum
        )


def pyCube_query42():
    with PythonTimer():
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


def pandas_query42_baseline1():
    with PythonTimer():
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


def pandas_query42_baseline2():
    with PythonTimer():
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

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        part2 = part1.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")
        part3 = part2.merge(mfgr_table, left_on="ca_mfgrkey", right_on="m_mfgrkey")

        supp_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supp_geo2 = supp_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        cust_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        cust_geo2 = cust_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        supp = supplier_table.merge(supp_geo2, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(cust_geo2, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part3, left_on="lo_partkey", right_on="p_partkey")
        merged_table3 = merged_table2.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table3.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

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


def pandas_query42_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    lo_supplycost,
                    y_year, 
                    region3.r_region AS r_region_c, 
                    nation1.n_nation AS n_nation,
                    region1.r_region AS r_region,
                    ca_category,
                    m_mfgr
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN nation AS nation1 ON city1.ci_nationkey = nation1.n_nationkey 
                    JOIN region AS region1 ON nation1.n_regionkey = region1.r_regionkey 
                    JOIN part AS part2 ON lineorder.lo_partkey = part2.p_partkey 
                    JOIN brand1 AS brand12 ON part2.p_brand1key = brand12.b_brand1key 
                    JOIN category AS category2 ON brand12.b_categorykey = category2.ca_categorykey 
                    JOIN mfgr AS mfgr2 ON category2.ca_mfgrkey = mfgr2.m_mfgrkey 
                    JOIN customer AS customer3 ON lineorder.lo_custkey = customer3.c_custkey 
                    JOIN city AS city3 ON customer3.c_citykey = city3.ci_citykey 
                    JOIN nation AS nation3 ON city3.ci_nationkey = nation3.n_nationkey 
                    JOIN region AS region3 ON nation3.n_regionkey = region3.r_regionkey 
                
            """,
                             conn
                             )

        engine.dispose()

        filtered_table = df[
            (df["r_region_c"] == "AMERICA     ")
            & (df["r_region"] == "AMERICA     ")
            & (
                    (df["y_year"] == 1997)
                    | (df["y_year"] == 1998)
            )
            & (
                    (df["m_mfgr"] == "MFGR#1")
                    | (df["m_mfgr"] == "MFGR#2")
            )
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="n_nation",
            columns=["y_year", "ca_category"],
            aggfunc=np.sum
        )


def pyCube_query43():
    with PythonTimer():
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


def pandas_query43_baseline1():
    with PythonTimer():
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


def pandas_query43_baseline2():
    with PythonTimer():
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

        date1 = date_table.merge(month_table, left_on="d_monthkey", right_on="mo_monthkey")
        date2 = date1.merge(year_table, left_on="mo_yearkey", right_on="y_yearkey")

        part1 = part_table.merge(brand_table, left_on="p_brand1key", right_on="b_brand1key")
        part2 = part1.merge(category_table, left_on="b_categorykey", right_on="ca_categorykey")

        supp_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        supp_geo2 = supp_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        cust_geo1 = city_table.merge(nation_table, left_on="ci_nationkey", right_on="n_nationkey")
        cust_geo2 = cust_geo1.merge(region_table, left_on="n_regionkey", right_on="r_regionkey")

        supp = supplier_table.merge(supp_geo2, left_on="s_citykey", right_on="ci_citykey")

        cust = customer_table.merge(cust_geo2, left_on="c_citykey", right_on="ci_citykey")

        merged_table1 = fact_table.merge(date2, left_on="lo_orderdate", right_on="d_datekey")
        merged_table2 = merged_table1.merge(part2, left_on="lo_partkey", right_on="p_partkey")
        merged_table3 = merged_table2.merge(supp, left_on="lo_suppkey", right_on="s_suppkey")
        merged_table = merged_table3.merge(cust, left_on="lo_custkey", right_on="c_custkey", suffixes=(None, "_c"))

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


def pandas_query43_baseline3():
    with PythonTimer():
        with engine.connect() as conn:
            df = pd.read_sql("""
                SELECT 
                    lo_revenue,
                    lo_supplycost,
                    y_year, 
                    city1.ci_city AS ci_city, 
                    nation1.n_nation AS n_nation,
                    region3.r_region AS r_region_c,
                    ca_category,
                    b_brand1
                FROM lineorder 
                    JOIN date ON lo_orderdate = d_datekey 
                    JOIN month ON d_monthkey = mo_monthkey 
                    JOIN year ON mo_yearkey = y_yearkey 
                    JOIN supplier AS supplier1 ON lineorder.lo_suppkey = supplier1.s_suppkey 
                    JOIN city AS city1 ON supplier1.s_citykey = city1.ci_citykey 
                    JOIN nation AS nation1 ON city1.ci_nationkey = nation1.n_nationkey 
                    JOIN part ON lo_partkey = p_partkey 
                    JOIN brand1 ON p_brand1key = b_brand1key 
                    JOIN category ON b_categorykey = ca_categorykey 
                    JOIN customer AS customer3 ON lineorder.lo_custkey = customer3.c_custkey 
                    JOIN city AS city3 ON customer3.c_citykey = city3.ci_citykey 
                    JOIN nation AS nation3 ON city3.ci_nationkey = nation3.n_nationkey 
                    JOIN region AS region3 ON nation3.n_regionkey = region3.r_regionkey 
                
            """,
                             conn
                             )

        engine.dispose()

        filtered_table = df[
            (df["r_region_c"] == "AMERICA     ")
            & (df["n_nation"] == "UNITED STATES  ")
            & (
                    (df["y_year"] == 1997)
                    | (df["y_year"] == 1998)
            )
            & (df["ca_category"] == "MFGR#14")
            ]
        filtered_table["profit"] = filtered_table.apply(lambda x: x.lo_revenue - x.lo_supplycost, axis=1)
        return filtered_table.pivot_table(
            values="profit",
            index="ci_city",
            columns=["y_year", "b_brand1"],
            aggfunc=np.sum
        )


# # query 1 from query_designs
# def pyCube_new_query1():
#     with PythonTimer():
#         view2 = view.measures(view.lo_extendedprice)
#         return view2.output(hack=True)
#
#
# # query 4 from query_designs
# def pyCube_new_query2():
#     with PythonTimer():
#         view2 = view.columns(view.supplier.region.r_region.members()) \
#                     .rows(view.part.mfgr.m_mfgr.members()) \
#                     .measures(view.lo_extendedprice)
#         test = view2.output()
#         return test
#
#
# # query 5 from query_designs
# def pyCube_new_query3():
#     with PythonTimer():
#         view2 = view.columns(view.supplier.region.r_region.members()) \
#                     .rows(view.part.mfgr.m_mfgr.members()) \
#                     .measures(view.lo_extendedprice,
#                               view.lo_quantity)
#         test = view2.output()
#         return test


# query 7 from query_designs
def pyCube_new_query1():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
            .rows(view.part.mfgr.m_mfgr.members()) \
            .measures(view.lo_extendedprice,
                      view.lo_quantity)
        return view2.output()


# query 8 from query_designs
def pyCube_new_query2():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
                    .rows(view.part.mfgr.m_mfgr.members()) \
                    .pages(view.customer.region.r_region.members()) \
                    .measures(view.lo_extendedprice,
                              view.lo_quantity)
        return view2.output()


# query 9 from query_designs
def pyCube_new_query3():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
                    .rows(view.part.mfgr.m_mfgr.members()) \
                    .pages(view.customer.nation.n_nation.members()) \
                    .measures(view.lo_extendedprice,
                              view.lo_quantity)
        return view2.output()


# query 10 from query_designs
def pyCube_new_query4():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
                    .rows(view.part.mfgr.m_mfgr.members()) \
                    .pages(view.customer.city.ci_city.members()) \
                    .measures(view.lo_extendedprice,
                              view.lo_quantity)
        return view2.output()


# query 11 from query_designs
def pyCube_new_query5():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
                    .rows(view.part.mfgr.m_mfgr.members()) \
                    .pages(view.customer.city.ci_city.members()) \
                    .measures(view.lo_extendedprice,
                              view.lo_quantity,
                              view.lo_revenue)
        return view2.output()


# query 12 from query_designs
def pyCube_new_query6():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
                    .rows(view.part.category.ca_category.members()) \
                    .pages(view.customer.city.ci_city.members()) \
                    .measures(view.lo_extendedprice,
                              view.lo_quantity,
                              view.lo_revenue)
        return view2.output()


# query 13 from query_designs
def pyCube_new_query7():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .pages(view.customer.city.ci_city.members()) \
            .measures(view.lo_extendedprice,
                      view.lo_quantity,
                      view.lo_revenue)
        return view2.output()


# query 14 from query_designs
def pyCube_new_query8():
    with PythonTimer():
        view2 = view.columns(view.supplier.city.ci_city.members()) \
            .rows(view.part.brand1.b_brand1.members()) \
            .pages(view.customer.city.ci_city.members()) \
            .measures(view.lo_extendedprice,
                      view.lo_quantity,
                      view.lo_revenue,
                      view.lo_supplycost)
        return view2.output()


def test():
    with PythonTimer():
        view2 = view.columns(view.part.part.p_color.members()) \
                    .rows(view.supplier.region.r_region.members()) \
                    .measures(view.lo_extendedprice)
        temp = view2.output()
        return temp


if sys.argv and len(sys.argv) == 2:
    try:
        python_timer = PythonTimer()
        db_timer = DBTimer()
        # eval(f"{sys.argv[1]}()")
        test()
        py_time = python_timer.elapsed_time()
        db_time = db_timer.elapsed_time()
        print(f"{sys.argv[1]}()")
        print(f"py_time: {py_time} -- db_time: {db_time}")
        print()
        sys.exit(0)
    except Exception as e:
        sys.exit(f"Exception: {e}")
