#!/bin/bash

# This script will call use_cases.py iteratively over every use case found in the file
# and will call it using time.
# The results should be saved to a file for later inspection.

source bin/activate
result=$(`python -m experiments.use_cases "hej"` 2>&1)
echo $result


