#!/bin/bash

source bin/activate

function run_time {
    echo $1 >> new_results
    /usr/bin/time -f "Wall clock time: %Eseconds\nMaximum resident set size of the process during its lifetime: %MKbytes" python -m experiments.use_cases $1 2>> new_results
}

function run_all_queryflights {
    for i in 1 2 3 4
    do
        if [[ $i -ne 3 ]]
        then
            for j in 1 2 3
            do
                if [[ $# -eq 1 ]]
                then
                    queryname=${1}${i}${j}
                else
                    queryname=${1}${i}${j}_baseline${2}
                fi
                run_time $queryname
            done
        else
            for j in 1 2 3 4
            do
                if [[ $# -eq 1 ]]
                then
                    queryname=${1}${i}${j}
                else
                    queryname=${1}${i}${j}_baseline${2}
                fi
                run_time $queryname
            done
        fi
    done
}

# run_all_queryflights "pyCube_query"
# run_all_queryflights "pandas_query" "1"
# run_all_queryflights "pandas_query" "2"
# run_all_queryflights "pandas_query" "3"

function run {
    query_names=$(shuf ${1})
    file_name=results_$(date "+%Y%m%d%H%M%S")
    for query_name in $(echo $query_names)
    do
        echo $query_name 
        echo $query_name >> $file_name
        /usr/bin/time -f "Wall clock time: %Eseconds\nMaximum resident set size of the process during its lifetime: %MKbytes" python -m experiments.use_cases $query_name 2>> $file_name
    done
}

function new_run_time {
    query_names=$(shuf ${1})
    file_name=$2
    for query_name in $(echo $query_names)
    do
        echo $query_name
        python -m experiments.use_cases $query_name 1>> results/$file_name
    done
}

function new_run_memory {
    query_names=$(shuf ${1})
    file_name=$2
    for query_name in $(echo $query_names)
    do
        echo $query_name 
        echo $query_name >> results/$file_name
        /usr/bin/time -f "Maximum resident set size of the process during its lifetime: %MKbytes" python -m experiments.use_cases $query_name 2>> results/$file_name
    done
}

new_run_time experiments_names_for_new_queries.txt $1
new_run_memory experiments_names_for_new_queries.txt $2
