#!/bin/bash

# To use this script call it with the dataset you want as an argument
# Datasets: 10, 1k, 20k, 100k
# Working directory must be MiniProj2/
# Text files must exist (run phase1.py first)
# Example call: ./delete_idx.sh 100k

cd IndexFiles/$1

rm ad.idx te.idx da.idx pr.idx

cd ../../

