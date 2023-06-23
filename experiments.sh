#!/bin/bash

# This script will call use_cases.py iteratively over every use case found in the file
# and will call it using time.
# The results should be saved to a file for later inspection.

source bin/activate

echo "pyCube_query11" >> results
/usr/bin/time python -m experiments.use_cases "pyCube_query11" 2>> results

echo "pandas_query11_baseline1" >> results
/usr/bin/time python -m experiments.use_cases "pandas_query11_baseline1" 2>> results

echo "pandas_query11_baseline2" >> results
/usr/bin/time python -m experiments.use_cases "pandas_query11_baseline2" 2>> results

echo "pandas_query11_baseline3" >> results
/usr/bin/time python -m experiments.use_cases "pandas_query11_baseline3" 2>> results




