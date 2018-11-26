#!/bin/bash

chmod +x break.pl
chmod +x phase1.py
chmod +x phase3.py
chmod +x clean.sh

./clean.sh

#------------------------------------ #

echo 
echo Parsing XML file

python3 phase1.py XML/$1.txt

#-------------------------------------#

echo Sorting .txt files

mkdir TextFiles

sort -u ads.txt > TextFiles/sorted_ads.txt
sort -u prices.txt > TextFiles/sorted_prices.txt
sort -u pdates.txt > TextFiles/sorted_pdates.txt
sort -u terms.txt > TextFiles/sorted_terms.txt

rm terms.txt ads.txt prices.txt pdates.txt

#-------------------------------------#

echo Formatting .txt files for db_load

perl break.pl <TextFiles/sorted_ads.txt> TextFiles/ads.txt
perl break.pl <TextFiles/sorted_prices.txt> TextFiles/prices.txt
perl break.pl <TextFiles/sorted_pdates.txt> TextFiles/pdates.txt
perl break.pl <TextFiles/sorted_terms.txt> TextFiles/terms.txt

rm TextFiles/sorted_ads.txt TextFiles/sorted_pdates.txt TextFiles/sorted_prices.txt TextFiles/sorted_terms.txt

#-------------------------------------#

echo Creating .idx files

mkdir IndexFiles

< TextFiles/ads.txt db_load -T -c duplicates=1 -t hash IndexFiles/ad.idx
< TextFiles/prices.txt db_load -T -c duplicates=1 -t btree IndexFiles/pr.idx
< TextFiles/terms.txt db_load -T -c duplicates=1 -t btree IndexFiles/te.idx
< TextFiles/pdates.txt db_load -T -c duplicates=1 -t btree IndexFiles/da.idx

#-------------------------------------#

echo Dumping .idx files

mkdir IndexDump

db_dump -p -f IndexDump/ad.txt IndexFiles/ad.idx
db_dump -p -f IndexDump/pr.txt IndexFiles/pr.idx
db_dump -p -f IndexDump/da.txt IndexFiles/da.idx
db_dump -p -f IndexDump/te.txt IndexFiles/te.idx

#-------------------------------------#

echo Done!
echo 