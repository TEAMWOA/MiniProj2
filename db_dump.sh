#!/bin/bash

# Dumps the index files created in phase 2 and the correct index files
# To use this script call it with the dataset you want as an argument
# Datasets: 10, 1k, 20k, 100k
# Working directory must be MiniProj2/
# Text files must exist (run phase1.py first)
# Example call: ./db_dump.sh 100k

cd IndexFiles/$1

rm -rf Dump
mkdir Dump

db_dump -p -f Dump/ad.txt ad.idx
db_dump -p -f Dump/pr.txt pr.idx
db_dump -p -f Dump/da.txt da.idx
db_dump -p -f Dump/te.txt te.idx

db_dump -p -f Dump/ad_c.txt Correct/ad.idx
db_dump -p -f Dump/pr_c.txt Correct/pr.idx
db_dump -p -f Dump/da_c.txt Correct/da.idx
db_dump -p -f Dump/te_c.txt Correct/te.idx

cd ../../

