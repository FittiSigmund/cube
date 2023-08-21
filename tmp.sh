#!/bin/bash

source bin/activate

for query_name in $(cat experiments_names_test.txt)
do
    echo $query_name
    python -m experiments.use_cases $query_name 1>> tmp.test
done
