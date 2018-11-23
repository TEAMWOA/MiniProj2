#!/bin/bash

# To use this script call it with the dataset you want as an argument
# Datasets: 10, 1k, 20k, 100k
# Working directory must be MiniProj2/
# Text files must exist (run phase1.py first)
# Example call: ./sort_files.sh 100k

cd TextFiles/$1

sort -u ads.txt > sorted_ads.txt
sort -u prices.txt > sorted_prices.txt
sort -u pdates.txt > sorted_pdates.txt
sort -u terms.txt > sorted_terms.txt

rm terms.txt ads.txt prices.txt pdates.txt

mv sorted_ads.txt ads.txt
mv sorted_prices.txt prices.txt
mv sorted_terms.txt terms.txt
mv sorted_pdates.txt pdates.txt

cd ../../

