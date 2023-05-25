# This is where I will implement all ssb query flights in both plain pandas and in pyCube
# The script will be parameterisated so that another bash script can invoke til script with parameter which decides
# which use case to invoke from this script.
# The bash script will run this script using GNU time iteratively over every use case.
# The bash script will save the results of time into files for later analysis.
import sys
import matplotlib.pyplot as plt

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
    result = view2.output(hack=True)
    hej = 1


def pyCube_query12():
    view2 = view.where(
        (view.date1.month.mo_yearmonthnum == 199401)
        & (view.lo_discount > 3)
        & (view.lo_discount < 5)
        & (view.lo_quantity > 25)
        & (view.lo_quantity < 35)
    ).measures(revenue=view.lo_extendedprice * view.lo_discount)
    result = view2.output(hack=True)
    hej = 1


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
    result = view2.output(hack=True)
    hej = 1


def pyCube_query21():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.category.ca_category == "MFGR#12")
        & (view.supplier.region.r_region == "AMERICA")
    ) \
        .measures(view.lo_revenue)
    result = view2.output()
    hej = 1


def pyCube_query22():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.brand1.b_brand1 > "MFGR#2220")
        & (view.part.brand1.b_brand1 < "MFGR#2229")
        & (view.supplier.region.r_region == "ASIA")
    ) \
        .measures(view.lo_revenue)
    result = view2.output()
    hej = 1


def pyCube_query23():
    view2 = view.columns(view.date1.year.y_year.members()) \
        .rows(view.part.brand1.b_brand1.members()) \
        .where(
        (view.part.brand1.b_brand1 == "MFGR#2339")
        & (view.supplier.region.r_region == "EUROPE")
    ) \
        .measures(view.lo_revenue)
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


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
    result = view2.output()
    hej = 1


pyCube_query31()
