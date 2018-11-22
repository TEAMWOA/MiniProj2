# Phase 2: Building Indexes

#(1) a hash index on ads.txt with ad id as key and the full ad record as data, 
#(2) a B+-tree index on terms.txt with term as key and ad id as data, 
#(3) a B+-tree index on pdates.txt with date as key and ad id, category and location as data, 
#(4) a B+-tree index on prices.txt with price as key and ad id, category and location as data
#
#
# 

from bsddb3 import db


def main():

    input_directory = "TextFiles/{}"
    output_directory = "IndexFiles/{}"

    #######################################################
    # (4) Create a B+ Tree Index on prices.txt pdates.txt
    # {price as key and ad id, category and location as data}
    prices = open(input_directory.format('prices.txt'), 'r')
    database = db.DB()
    database.set_flags(db.DB_DUP)
    database.open(output_directory.format('pr.idx'), None, db.DB_BTREE, db.DB_CREATE)
    curs = database.cursor()

    for line in prices:
        a=line.split(":")
        price=a[0] #key
        ID_CAT_LOC=line[1] #data
        database.put(x.encode("utf-8"),ID_CAT_LOC)
    curs.close()
    database.close()
    #######################################################
    # (3) Create a B+ Tree Index on pdates.txt
    # {date as key and ad id, category and location as data}
    dates = open(input_directory.format('pdates.txt'), 'r')
    database = db.DB()
    database.set_flags(db.DB_DUP)
    database.open(output_directory.format('da.idx'), None, db.DB_BTREE, db.DB_CREATE)
    curs = database.cursor()

    for line in dates:
        dateisKey=line[:10] #key
        ID_CAT_LOC=line[11:] #data
        database.put(x.encode("utf-8"),ID_CAT_LOC)
    curs.close()
    database.close()
    #######################################################
    # (2) Create a B+ Tree Index on terms.txt
    # {term as key and ad id as data}
    terms = open(input_directory.format('terms.txt'), 'r')
    database = db.DB()
    database.set_flags(db.DB_DUP)
    database.open(output_directory.format('te.idx'), None, db.DB_BTREE, db.DB_CREATE)
    cur = database.cursor()
    
    for line in terms:
        a=line.split(":")
        term=a[0] #key 
        adID=a[1] #data
        database.put(term.encode("utf-8"), adID)
    database.close()
    cur.close()
    ##########################################################
    # (1) Create Hash Index on ads.txt
    # {ad id as key and the full ad record as data}
    ads = open(input_directory.format('ads.txt'), 'r')
    database = db.DB()
    database.set_flags(db.DB_DUP)
    database.open(output_directory.format('ad.idx'), None, db.DB_HASH, db.DB_CREATE)
    cur = database.cursor()

    for line in ads:
        a = line.split(":")
        adID=a[0] #key
        adRecord=a[1] #data
        database.put(adID.encode("utf-8"),adRecord)
    database.close()
    cur.close()

main()
