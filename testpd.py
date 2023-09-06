import pandas as pd
import psycopg2

from sqlalchemy import create_engine, text


query = """
    SELECT
        mo.mo_month, ca.ca_category, ci.ci_city, SUM(extendedprice) AS extendedprice, SUM(quantity) AS quantity
    FROM
        lineorder
        JOIN date AS d ON lineorder.orderdate = d.d_datekey
        JOIN month AS mo ON d.d_monthkey = mo.mo_monthkey
        JOIN year AS y ON mo.mo_yearkey = y.y_yearkey
        JOIN part AS p ON lineorder.partkey = p.p_partkey
        JOIN brand1 AS b ON p.p_brand1key = b.b_brand1key
        JOIN category AS ca ON b.b_categorykey = ca.ca_categorykey
        JOIN mfgr AS m ON ca.ca_mfgrkey = m.m_mfgrkey
        JOIN customer AS c ON lineorder.custkey = c.c_custkey
        JOIN city AS ci ON c.c_citykey = ci.ci_citykey
        JOIN nation AS n ON ci.ci_nationkey = n.n_nationkey
        JOIN region AS r ON n.n_regionkey = r.r_regionkey
    WHERE
        mo.mo_month IN ('January', 'February') AND
        ca.ca_category IN ('MFGR#44', 'MFGR#33') AND
        ci.ci_city IN ('ARGENTINA3', 'ARGENTINA1')
    GROUP BY
        mo.mo_month, y.y_year,
        ca.ca_category, m.m_mfgr,
        ci.ci_city, n.n_nation, r.r_region
"""

table = [
    ['January', 'Blouse', 'Aalborg', 1, 1],
    ['January', 'Pants', 'Aalborg', 2, 2],
    ['January', 'Blouse', 'Aarhus', 3, 3],
    ['January', 'Pants', 'Aarhus', 4, 4],
    ['February', 'Blouse', 'Aalborg', 5, 5],
    ['February', 'Pants', 'Aalborg', 6, 6],
    ['February', 'Blouse', 'Aarhus', 7, 7],
    ['February', 'Pants', 'Aarhus', 8, 8]
]


def conv(table):
    data = {}
    for row in table:
        if (row[0], row[2]) in data:
            values = data[row[0], row[2]]
            values[row[1]] = tuple(row[3:])
            data[row[0], row[2]] = values
        else:
            data[(row[0], row[2])] = {row[1]: tuple(row[3:])}
        # if axes[0] not in df.columns:
        #     df.insert(len(df.columns), axes[0], [()] * len(df.index))
        # if axes[1] not in axis1:
        #     axis1.append(axes[1])
        # if axes[2] not in axis2:
        #     axis2.append(axes[2])

        # while i < 3:
        #     if row[i] in dictionaries[f"d{i}"]:
        #         row[i] = dictionaries[f"d{i}"][row[i]]
        #     else:
        #         dictionaries[f"d{i}"][row[i]] = counters[f"c{i}"]
        #         row[i] = dictionaries[f"d{i}"][row[i]]
        #         counters[f"c{i}"] = counters[f"c{i}"] + 1
        #     i = i + 1

    return pd.DataFrame(data)



# conn = psycopg2.connect(
#     dbname="ssb",
#     user="sigmundur",
#     password=""
# )
#
# with conn:
#     with conn.cursor() as curs:
#         curs.execute("""
#             SELECT
#                 mo.mo_month, ca.ca_category, ci.ci_city, SUM(extendedprice), SUM(quantity)
#             FROM
#                 lineorder
#                 JOIN date AS d ON lineorder.orderdate = d.d_datekey
#                 JOIN month AS mo ON d.d_monthkey = mo.mo_monthkey
#                 JOIN year AS y ON mo.mo_yearkey = y.y_yearkey
#                 JOIN part AS p ON lineorder.partkey = p.p_partkey
#                 JOIN brand1 AS b ON p.p_brand1key = b.b_brand1key
#                 JOIN category AS ca ON b.b_categorykey = ca.ca_categorykey
#                 JOIN mfgr AS m ON ca.ca_mfgrkey = m.m_mfgrkey
#                 JOIN customer AS c ON lineorder.custkey = c.c_custkey
#                 JOIN city AS ci ON c.c_citykey = ci.ci_citykey
#                 JOIN nation AS n ON ci.ci_nationkey = n.n_nationkey
#                 JOIN region AS r ON n.n_regionkey = r.r_regionkey
#             WHERE
#                 mo.mo_month IN ('January', 'February') AND
#                 ca.ca_category IN ('MFGR#44', 'MFGR#33') AND
#                 ci.ci_city IN ('ARGENTINA3', 'ARGENTINA1')
#             GROUP BY
#                 mo.mo_month, y.y_year,
#                 ca.ca_category, m.m_mfgr,
#                 ci.ci_city, n.n_nation, r.r_region
#         """)
#         result = curs.fetchall()
#         test = conv(result)
#         hej = 1


connection_string = ""

def DataFrameAlgorithm3():
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
        df["Measures"] = df[df.columns[3:]].apply(lambda x: (x[0], x[1]), axis=1)
        final_df = df.pivot(columns=["Month", "City"], index="Category", values="Measures")
        return final_df



    # "postgresql+psycopg2://sigmundur:@localhost/ssb"
    # df1 = df.drop_duplicates(subset=["mo_month", "ca_category", "ci_city"])
