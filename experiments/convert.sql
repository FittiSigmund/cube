BEGIN;

-- Create tables for default ssb

CREATE TABLE part (
    partkey     INTEGER PRIMARY KEY,
    name        VARCHAR(22),
    mfgr        CHAR(6),
    category    CHAR(7),
    brand1      CHAR(9),
    color       VARCHAR(11),
    type        VARCHAR(25),
    size        SMALLINT,
    container   CHAR(10)
);

CREATE TABLE supplier (
    suppkey     INTEGER PRIMARY KEY,
    name        CHAR(25),
    address     VARCHAR(25),
    city        CHAR(10),
    nation      CHAR(15),
    region      CHAR(12),
    phone       CHAR(15)
);

CREATE TABLE customer (
    custkey     INTEGER PRIMARY KEY,
    name        VARCHAR(25),
    address     VARCHAR(25),
    city        CHAR(10),
    nation      CHAR(15),
    region      CHAR(12),
    phone       CHAR(15),
    mktsegment  CHAR(10)
);

CREATE TABLE date (
    datekey             INTEGER PRIMARY KEY,
    date                CHAR(19),
    dayofweek           CHAR(9),
    month               CHAR(9),
    year                SMALLINT,
    yearmonthnum        INTEGER,
    yearmonth           CHAR(7),
    daynuminweek        SMALLINT,
    daynuminmonth       SMALLINT,
    daynuminyear        SMALLINT,
    monthnuminyear      SMALLINT,
    weeknuminyear       SMALLINT,
    sellingseason       CHAR(12),
    lastdayinweekfl     BOOLEAN,
    lastdayinmonthfl    BOOLEAN,
    holidayfl           BOOLEAN,
    weekdayfl           BOOLEAN
);

CREATE TABLE lineorder (
    lo_orderkey            INTEGER,
    lo_linenumber          SMALLINT,
    lo_custkey             INTEGER REFERENCES customer(custkey),
    lo_partkey             INTEGER REFERENCES part(partkey),
    lo_suppkey             INTEGER REFERENCES supplier(suppkey),
    lo_orderdate           INTEGER REFERENCES date(datekey),
    lo_orderpriority       CHAR(15),
    lo_shippriority        CHAR(1),
    lo_quantity            SMALLINT,
    lo_extendedprice       INTEGER,
    lo_ordtotalprice       INTEGER,
    lo_discount            SMALLINT,
    lo_revenue             INTEGER,
    lo_supplycost          INTEGER,
    lo_tax                 SMALLINT,
    lo_commitdate          INTEGER REFERENCES date(datekey),
    lo_shipmode            CHAR(10),
    PRIMARY KEY (lo_orderkey, lo_linenumber)
);

-- Load generated ssb data

\copy supplier FROM '/home/sigmundur/work/cubeProject/cube/experiments/data/supplier.tbl' DELIMITER '|' CSV;
\copy customer FROM '/home/sigmundur/work/cubeProject/cube/experiments/data/customer.tbl' DELIMITER '|' CSV;
\copy part FROM '/home/sigmundur/work/cubeProject/cube/experiments/data/part.tbl' DELIMITER '|' CSV;
\copy date FROM '/home/sigmundur/work/cubeProject/cube/experiments/data/date.tbl' DELIMITER '|' CSV;
\copy lineorder FROM '/home/sigmundur/work/cubeProject/cube/experiments/data/lineorder.tbl' DELIMITER '|' CSV;




-- Create tables for snowflake ssb

CREATE TABLE mfgr (
  m_mfgrkey SMALLSERIAL PRIMARY KEY,
  m_mfgr CHAR(6)
);

CREATE TABLE category (
  ca_categorykey SMALLSERIAL PRIMARY KEY,
  ca_category CHAR(7),
  ca_mfgrkey SMALLSERIAL REFERENCES mfgr(m_mfgrkey)
);

CREATE TABLE brand1 (
  b_brand1key SMALLSERIAL PRIMARY KEY,
  b_brand1 CHAR(9),
  b_categorykey SMALLSERIAL REFERENCES category(ca_categorykey)
);

CREATE TABLE region (
    r_regionkey SMALLSERIAL PRIMARY KEY,
    r_region CHAR(12)
);

CREATE TABLE nation (
    n_nationkey SMALLSERIAL PRIMARY KEY,
    n_nation CHAR(15),
    n_regionkey SMALLSERIAL REFERENCES region(r_regionkey)
);

CREATE TABLE city (
    ci_citykey SMALLSERIAL PRIMARY KEY,
    ci_city CHAR(10),
    ci_nationkey SMALLSERIAL REFERENCES nation(n_nationkey)
);

CREATE TABLE new_supplier (
    s_suppkey INTEGER PRIMARY KEY,
    s_name    CHAR(25),
    s_address VARCHAR(25),
    s_phone   CHAR(15),
    s_citykey SMALLSERIAL REFERENCES city(ci_citykey)
);

CREATE TABLE new_part (
  p_partkey   INTEGER PRIMARY KEY,
  p_name      VARCHAR(22),
  p_color     VARCHAR(11),
  p_type      VARCHAR(25),
  p_size      SMALLINT,
  p_container CHAR(10),
  p_brand1key SMALLSERIAL REFERENCES brand1(b_brand1key)
);

CREATE TABLE new_customer (
    c_custkey INTEGER PRIMARY KEY,
    c_name VARCHAR(25),
    c_address VARCHAR(25),
    c_phone CHAR(15),
    c_mktsegment CHAR(10),
    c_citykey SMALLSERIAL REFERENCES city(ci_citykey)
);

CREATE TABLE year (
    y_yearkey SMALLSERIAL PRIMARY KEY,
    y_year SMALLINT
);

CREATE TABLE month (
    mo_monthkey SMALLSERIAL PRIMARY KEY,
    mo_month CHAR(9),
    mo_yearmonthnum INTEGER,
    mo_yearmonth CHAR(7),
    mo_monthnuminyear SMALLINT,
    mo_yearkey SMALLSERIAL REFERENCES year(y_yearkey)
);

CREATE TABLE day (
    d_daykey INTEGER PRIMARY KEY,
    d_dayofweek CHAR(9),
    d_daynuminweek SMALLINT,
    d_daynuminmonth SMALLINT,
    d_sellingseason CHAR(12),
    d_lastdayinweekfl BOOLEAN,
    d_lastdayinmonthfl BOOLEAN,
    d_holidayfl BOOLEAN,
    d_weekdayfl BOOLEAN,
    d_daynuminyear SMALLINT,
    d_monthkey SMALLSERIAL REFERENCES month(mo_monthkey)
);


-- Convert and load ssb data from star schema to snowflake schema

INSERT INTO mfgr (m_mfgr)
    SELECT mfgr
    FROM 
        part
    GROUP BY 
        mfgr;

INSERT INTO category (ca_category, ca_mfgrkey) 
    SELECT 
        p.category, m.m_mfgrkey
    FROM 
        part AS p
    JOIN
        mfgr AS m
    ON
        p.mfgr = m.m_mfgr
    GROUP BY p.category, m.m_mfgrkey;

INSERT INTO brand1 (b_brand1, b_categorykey) 
    SELECT p.brand1, ca.ca_categorykey
    FROM 
        part AS p
    JOIN
        category AS ca ON p.category = ca.ca_category
    JOIN
        mfgr AS m ON p.mfgr = m.m_mfgr
    GROUP BY
        p.brand1, ca.ca_categorykey;

INSERT INTO new_part (p_partkey, p_name, p_color, p_type, p_size, p_container, p_brand1key)
    SELECT p.partkey, p.name, p.color, p.type, p.size, p.container, b.b_brand1key
    FROM 
        part AS p,
        brand1 AS b 
    JOIN
        category AS ca ON b.b_categorykey = ca.ca_categorykey
    JOIN
        mfgr AS m ON ca.ca_mfgrkey = m.m_mfgrkey
    WHERE
        p.brand1 = b.b_brand1 AND
        p.category = ca.ca_category AND
        p.mfgr = m.m_mfgr;

INSERT INTO region (r_region) 
    SELECT region 
    FROM supplier 
    GROUP BY region;

INSERT INTO nation (n_nation, n_regionkey)
    SELECT 
        s.nation, r.r_regionkey
    FROM 
        supplier AS s,
        region AS r
    WHERE
        s.region = r.r_region
    GROUP BY
        s.nation, r.r_regionkey;

INSERT INTO city (ci_city, ci_nationkey)
    SELECT
        s.city, n.n_nationkey
    FROM
        supplier AS s,
        nation AS n
    WHERE
        s.nation = n.n_nation
    GROUP BY
        s.city, n.n_nationkey;

INSERT INTO new_supplier (s_suppkey, s_name, s_address, s_phone, s_citykey)
    SELECT
        s.suppkey, s.name, s.address, s.phone, ci.ci_citykey
    FROM 
        supplier AS s,
        city AS ci
    JOIN
        nation as n ON ci.ci_nationkey = n.n_nationkey
    JOIN
        region as r ON n.n_regionkey = r.r_regionkey
    WHERE
        s.city = ci.ci_city AND
        s.nation = n.n_nation AND
        s.region = r.r_region;


INSERT INTO new_customer (c_custkey, c_name, c_address, c_phone, c_mktsegment, c_citykey)
    SELECT 
        c.custkey, c.name, c.address, c.phone, c.mktsegment, ci.ci_citykey
    FROM 
        customer AS c,
        city AS ci
    JOIN
        nation AS n ON ci.ci_nationkey = n.n_nationkey
    JOIN
        region AS r ON n.n_regionkey = r.r_regionkey
    WHERE
        c.city = ci.ci_city AND
        c.nation = n.n_nation AND
        c.region = r.r_region;

INSERT INTO year (y_year)
    SELECT 
        year
    FROM 
        date
    GROUP BY
        year
    ORDER BY
        year;

INSERT INTO month (mo_month, mo_yearmonthnum, mo_yearmonth, mo_monthnuminyear, mo_yearkey)
    SELECT
        d.month, d.yearmonthnum, d.yearmonth, d.monthnuminyear, y.y_yearkey
    FROM 
        date AS d,
        year AS y
    WHERE
        d.year = y.y_year
    GROUP BY
        d.month, d.yearmonthnum, d.yearmonth, d.monthnuminyear, y.y_yearkey
    ORDER BY
        d.yearmonthnum;

INSERT INTO day (d_daykey, d_dayofweek, d_daynuminweek, d_daynuminmonth, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl, d_daynuminyear, d_monthkey)
    SELECT 
        d.datekey, d.dayofweek, d.daynuminweek, d.daynuminmonth, d.sellingseason, d.lastdayinweekfl::boolean, d.lastdayinmonthfl::boolean, d.holidayfl::boolean, d.weekdayfl::boolean, d.daynuminyear, mo.mo_monthkey
    FROM
        date AS d
    JOIN
        month AS mo ON d.month = mo.mo_month
    JOIN
        year AS y ON mo.mo_yearkey = y.y_yearkey
    WHERE
        d.year = y.y_year
    GROUP BY
        d.datekey, d.month, d.dayofweek, d.daynuminweek, d.daynuminmonth, d.sellingseason, d.lastdayinweekfl, d.lastdayinmonthfl, d.holidayfl, d.weekdayfl, d.daynuminyear, mo.mo_monthkey
    ORDER BY 
        d.datekey;


-- Remove foreign key constraints pointing to old star schema dimension tables

ALTER TABLE 
    lineorder 
DROP CONSTRAINT 
    lineorder_lo_custkey_fkey, 
DROP CONSTRAINT
    lineorder_lo_partkey_fkey,
DROP CONSTRAINT
    lineorder_lo_suppkey_fkey,
DROP CONSTRAINT
    lineorder_lo_orderdate_fkey,
DROP CONSTRAINT
    lineorder_lo_commitdate_fkey;


-- Add foreign key constraints pointing to new snowflake schema dimension tables

ALTER TABLE
    lineorder
ADD CONSTRAINT
    lineorder_lo_custkey_fkey FOREIGN KEY (lo_custkey) REFERENCES new_customer(c_custkey),
ADD CONSTRAINT
    lineorder_lo_partkey_fkey FOREIGN KEY (lo_partkey) REFERENCES new_part(p_partkey),
ADD CONSTRAINT
    lineorder_lo_suppkey_fkey FOREIGN KEY (lo_suppkey) REFERENCES new_supplier(s_suppkey),
ADD CONSTRAINT
    lineorder_lo_orderdate_fkey FOREIGN KEY (lo_orderdate) REFERENCES day(d_daykey),
ADD CONSTRAINT
    lineorder_lo_commitdate_fkey FOREIGN KEY (lo_commitdate) REFERENCES day(d_daykey);


-- Delete old star schema dimension tables
DROP TABLE customer, supplier, part, date;

-- Rename new snowflake schema dimension tables to names previously taken by the old dimension tables
ALTER TABLE new_customer RENAME TO customer;
ALTER TABLE new_supplier RENAME TO supplier;
ALTER TABLE new_part RENAME TO part;

-- Lazily correct naming mistakes I made further up in the script :)
ALTER TABLE day RENAME d_daykey TO d_datekey;
ALTER TABLE day RENAME TO date;


COMMIT;
