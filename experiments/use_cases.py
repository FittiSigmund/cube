# This is where I will implement all ssb query flights in both plain pandas and in pyCube
# The script will be parameterisated so that another bash script can invoke til script with parameter which decides
# which use case to invoke from this script.
# The bash script will run this script using GNU time iteratively over every use case.
# The bash script will save the results of time into files for later analysis.
import sys

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
                       & (view.lo_quantity < 25))\
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


pyCube_query13()
