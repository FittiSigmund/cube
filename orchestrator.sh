#!/bin/bash

cd experiments/ssb-dbgen
./dbgen -s 1 -T a
sed 's/.$//' supplier.tbl > ../data/supplier.tbl
sed 's/.$//' customer.tbl > ../data/customer.tbl
sed 's/.$//' part.tbl > ../data/part.tbl
sed 's/.$//' date.tbl > ../data/date.tbl
sed 's/.$//' lineorder.tbl > ../data/lineorder.tbl
rm supplier.tbl customer.tbl part.tbl date.tbl lineorder.tbl
cd ../..
psql -d ssb_snowflake -f experiments/bobby.sql
psql -d ssb_snowflake -f experiments/convert.sql

