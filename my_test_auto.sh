#!/bin/bash

echo "Script for my_test automatization"
echo "------------------------------------------------"

echo "Getting python3 executable loc"
python_exec_loc=$(which python3)
if [ $? -eq 0 ]; then echo "OK"; else echo "Problem getting python3 exec location"; exit 1; fi
echo "$python_exec_loc"
echo "------------------------------------------------"

echo "Running My_test"
$python_exec_loc my_test.py
if [ $? -eq 0 ]; then echo "My_test Working correctly"; else echo "My_test FAILED"; exit 1; fi
echo "------------------------------------------------"
