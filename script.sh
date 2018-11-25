#!/bin/bash

echo 
echo Parsing XML file
python3 phase1.py $1

echo Sorting .txt files

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

echo Formatting .txt files for db_load
perl break.pl <TextFiles/$1/ads.txt> TextFiles/$1/b_ads.txt
perl break.pl <TextFiles/$1/prices.txt> TextFiles/$1/b_prices.txt
perl break.pl <TextFiles/$1/pdates.txt> TextFiles/$1/b_pdates.txt
perl break.pl <TextFiles/$1/terms.txt> TextFiles/$1/b_terms.txt

cd TextFiles/$1/
mv b_ads.txt ads.txt
mv b_prices.txt prices.txt
mv b_pdates.txt pdates.txt
mv b_terms.txt terms.txt

cd ../../

echo Creating .idx files
< TextFiles/$1/ads.txt db_load -T -c duplicates=1 -t hash IndexFiles/$1/ad.idx
< TextFiles/$1/prices.txt db_load -T -c duplicates=1 -t btree IndexFiles/$1/pr.idx
< TextFiles/$1/terms.txt db_load -T -c duplicates=1 -t btree IndexFiles/$1/te.idx
< TextFiles/$1/pdates.txt db_load -T -c duplicates=1 -t btree IndexFiles/$1/da.idx

echo Dumping .idx files

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

echo Done!
echo 