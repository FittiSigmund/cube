#!/bin/bash

source bin/activate

function run_time {
    echo $1 >> new_results
    /usr/bin/time -f "Wall clock time: %Eseconds\nUser CPU time: %Useconds\nSystem CPU time: %Sseconds\nMaximum resident set size of the process during its lifetime: %MKbytes\nAverage resident set size of the process: %tKbytes\nAverage total (data+stack+text) memory use of the process: %KKbytes" python -m experiments.use_cases $1 2>> new_results
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

run_all_queryflights "pyCube_query"
run_all_queryflights "pandas_query" "1"
run_all_queryflights "pandas_query" "2"
run_all_queryflights "pandas_query" "3"
