# This is where I will implement all ssb query flights in both plain pandas and in pyCube
# The script will be parameterisated so that another bash script can invoke til script with parameter which decides
# which use case to invoke from this script.
# The bash script will run this script using GNU time iteratively over every use case.
# The bash script will save the results of time into files for later analysis.
import sys
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


def pyCube_query11():
    view2 = view.where(
        (view.date1.year.y_year == 1993)
        & (view.lo_discount > 0)
        & (view.lo_discount < 4)
        & (view.lo_quantity < 25)) \
        .measures(revenue=view.lo_extendedprice * view.lo_discount)
    return view2.output(hack=True)


# Load data into main memory
#   as several different df tables and then join them together in memory
# Do projection
# Filter data
# Group by
# (maybe another filter step)
# (maybe do data clean up (removing missing data and similar))
# Sort the df
# Return df and compare pandas df and pyCube df with pandasdf.equals(pyCubedf)
def pandas_query11():
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


def pyCube_query12():
    view2 = view.where(
        (view.date1.month.mo_yearmonthnum == 199401)
        & (view.lo_discount > 3)
        & (view.lo_discount < 7)
        & (view.lo_quantity > 25)
        & (view.lo_quantity < 36)
    ).measures(revenue=view.lo_extendedprice * view.lo_discount)
    return view2.output(hack=True)


def pandas_query12():
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


def pyCube_query13():
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


def pandas_query13():
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


def pyCube_query21():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.category.ca_category == "MFGR#12")
        & (view.supplier.region.r_region == "AMERICA")
    ) \
        .measures(view.lo_revenue)
    return view2.output()


def pandas_query21():
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


def pyCube_query22():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.brand1.b_brand1 > "MFGR#2220")
        & (view.part.brand1.b_brand1 < "MFGR#2229")
        & (view.supplier.region.r_region == "ASIA")
    ) \
        .measures(view.lo_revenue)
    return view2.output()


def pandas_query22():
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


def pyCube_query23():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.brand1.b_brand1 == "MFGR#2339")
        & (view.supplier.region.r_region == "EUROPE")
    ) \
        .measures(view.lo_revenue)
    return view2.output()


def pandas_query23():
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


def pyCube_query31():
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


def pandas_query31():
    with engine.connect() as conn:
        fact_table = pd.read_sql("lineorder", conn, columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
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


def pyCube_query32():
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


def pandas_query32():
    with engine.connect() as conn:
        fact_table = pd.read_sql("lineorder", conn, columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
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
    merged_table = temp2.merge(nation_table, left_on="ci_nationkey_c", right_on="n_nationkey", suffixes=(None, "_c"))
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


def pyCube_query33():
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


def pandas_query33():
    with engine.connect() as conn:
        fact_table = pd.read_sql("lineorder", conn, columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
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


def pyCube_query34():
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


def pandas_query34():
    with engine.connect() as conn:
        fact_table = pd.read_sql("lineorder", conn, columns=["lo_orderdate", "lo_suppkey", "lo_custkey", "lo_revenue"])
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


def pyCube_query41():
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


def pandas_query41():
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


def pyCube_query42():
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


def pandas_query42():
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


def pyCube_query43():
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


def pandas_query43():
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


def test():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.supplier.city.ci_city.members()) \
        .pages(view.part.brand1.b_brand1.members()) \
        .where(
        (
                (view.customer.region.r_region == "AMERICA")
                & (view.supplier.nation.n_nation == "UNITED STATES")
                & (view.date1.year.y_year == 1997)
        )
        | (
                (view.date1.year.y_year == 1998)
                & (view.part.category.ca_category == "MFGR#14")
        )
    ) \
        .measures(profit=view.lo_revenue - view.lo_supplycost)
    result = view2.output()
    hej = 1


# Template for comparing pyCube and pandas in query flight 1
def compare_query_flight1() -> bool:
    pyCube_result = pyCube_query21()
    pandas_result = pandas_query21()
    return pyCube_result.equals(pandas_result)


# Template for comparing pyCube and pandas in query flight 2
def compare_query_flight2() -> bool:
    pyCube_result = pyCube_query21()
    pandas_result = pandas_query21()
    pyCube_result.columns = pyCube_result.columns.droplevel(level=1)
    return pyCube_result.equals(pandas_result)


# Template for comparing pyCube and pandas in query flight 3
def compare_query_flight3() -> bool:
    pyCube_result = pyCube_query21()
    pandas_result = pandas_query21()
    pyCube_result.columns = pyCube_result.columns.droplevel(level=2)
    # May need to add/remove spaces in "by" parameter
    result1 = pyCube_result.sort_values(by="CHINA          ", axis=1)
    result2 = pandas_result.sort_values(by="CHINA          ", axis=1)
    return result1.equals(result2)


pyCube_result = pyCube_query42()
pandas_result = pandas_query42()
pyCube_result.columns = pyCube_result.columns.droplevel(level=2)
result = pyCube_result.equals(pandas_result)
hej = 1
