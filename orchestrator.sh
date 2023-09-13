#!/bin/bash

function first_part {
    cd experiments/ssb-dbgen
    ./dbgen -s $1 -T s
    ./dbgen -s $1 -T p
    ./dbgen -s $1 -T d
    ./dbgen -s $1 -T c
    ./dbgen -s $1 -T l
    sed 's/.$//' supplier.tbl > ../data/supplier.tbl
    sed 's/.$//' customer.tbl > ../data/customer.tbl
    sed 's/.$//' part.tbl > ../data/part.tbl
    sed 's/.$//' date.tbl > ../data/date.tbl
    sed 's/.$//' lineorder.tbl > ../data/lineorder.tbl
    rm supplier.tbl customer.tbl part.tbl date.tbl lineorder.tbl
    cd ../..
    psql -d ssb_snowflake -f experiments/bobby.sql
    psql -d ssb_snowflake -f experiments/convert.sql
}

function second_part {
    source bin/activate
    current_date=$(date "+%Y%m%d%H%M%S")
    time_file_name=time_results_$current_date
    memory_file_name=memory_results_$current_date
    final_file_name=final_results_$current_date
    ./experiments.sh $time_file_name $memory_file_name
    python -m result_time_converter results/$time_file_name 1>> results/final_results/$final_file_name
    python -m result_memory_converter results/$memory_file_name 1>> results/final_results/$final_file_name
}

# first_part $1
second_part
