#!/bin/bash


source bin/activate

file_name=final_results_$(date "+%Y%m%d%H%M%S")
python -m result_time_converter $1 1>> $file_name
