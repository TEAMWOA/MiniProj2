import os
import re
from bsddb3 import db
from datetime import datetime as dt


class QueryParser:
    def __init__(self):

        self.priceDB = db.DB()
        self.priceDB.open("IndexFiles/pr.idx", None, db.DB_BTREE, db.DB_CREATE)
        self.priceCursor = self.priceDB.cursor()

        self.adDB = db.DB()
        self.adDB.open("IndexFiles/ad.idx", None, db.DB_HASH, db.DB_CREATE)
        self.adCursor = self.adDB.cursor()

        self.dateDB = db.DB()
        self.dateDB.open("IndexFiles/da.idx", None, db.DB_BTREE, db.DB_CREATE)
        self.dateCursor = self.dateDB.cursor()

        self.termDB = db.DB()
        self.termDB.open("IndexFiles/te.idx", None, db.DB_BTREE, db.DB_CREATE)
        self.termCursor = self.termDB.cursor()

        self.query_data = {
            "price >"    : [],
            "price <"    : [],
            "price >="   : [],
            "price <="   : [],
            "price ="    : [],

            "date >"     : [],
            "date <"     : [],
            "date >="    : [],
            "date <="    : [],
            "date ="     : [],

            "locations"  : [],
            "categories" : [],
            "terms"      : [],
            "terms%"     : []
        }

        self.regexes = {
            "dateQuery"      : "date[ ]*(<|>|<=|>=|=)[ ]*[0-9]{4}/[0-9]{2}/[0-9]{2}",
            "priceQuery"     : "price[ ]*(<|>|<=|>=|=)[ ]*[0-9]+",
            "locationQuery"  : "location[ ]*=[ ]*[0-9a-zA-Z_-]+",
            "catQuery"       : "cat[ ]*=[ ]*[0-9a-zA-Z_-]+",
            "termQuery"      : "([0-9a-zA-Z_-]+%|[0-9a-zA-Z_-]+)"
        }

        self.reserved_keywords = ["price", "cat", "location", "date", "output"]
        self.priceMatches = []
        self.priceMatches2 = []
        self.dateMatches = []
        self.dateMatches2 = []
        
        #Location adID Matches
        self.LOC_adIDs = []
        
        #Category adID Matches
        self.CAT_adIDS = [] 
        
        #Term adID Matches
        self.TERM_adIds = []

    def close_databases(self):

        self.priceDB.close()
        self.priceCursor.close()

        self.adDB.close()
        self.adCursor.close()
        
        self.dateDB.close()
        self.dateCursor.close()
        
        self.termDB.close()
        self.termCursor.close()

    
    def set_query(self, query):

        self.original_query = query
        self.query = query

        self.query_data = {
            "price >"    : [],
            "price <"    : [],
            "price >="   : [],
            "price <="   : [],
            "price ="    : [],

            "date >"     : [],
            "date <"     : [],
            "date >="    : [],
            "date <="    : [],
            "date ="     : [],

            "locations"  : [],
            "categories" : [],
            "terms"      : [],
            "terms%"     : []
        }

        self.parse()

#------------------------------------------------------------------------#

    def parse(self):
        
        # Search for price queries
        result = re.search(self.regexes["priceQuery"], self.query)
        if result:
            result = result.group(0)
            print(result)
            self.price_query(result) 
            list1 = set(self.priceMatches)
            self.priceMatches.clear()
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["priceQuery"], self.query)
            
            #if there is a second price range
            if result:
                result = result.group(0)
                print(result)
                self.price_query(result)
            list2 = set(self.priceMatches)
            priceMatchList = list1 & list2        
            print(priceMatchList)
        # Search for date queries
        result = re.search(self.regexes["dateQuery"], self.query)
        if result:
            
            result = result.group(0)
            print(result)
            self.date_query(result)
            list1 = set(self.dateMatches)
            #self.dateMatches.clear()
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["dateQuery"], self.query)
            
            if result:
                self.dateMatches.clear()
                result = result.group(0)
                print(result)
                self.date_query(result)
            list2 = set(self.dateMatches)
            dateMatchList = list1 & list2 
            
            if dateMatchList:
                priceDateMatchList = priceMatchList & dateMatchList
            else:
                priceDateMatchList = list1
        

        # Search for location queries
        result = re.search(self.regexes["locationQuery"], self.query)
        while result:
            result = result.group(0)
            self.location_query(result)
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["locationQuery"], self.query)

        # Search for category queries
        result = re.search(self.regexes["catQuery"], self.query)
        while result:
            result = result.group(0)
            self.category_query(result)
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["catQuery"], self.query)

        # Search for term queries
        result = re.search(self.regexes["termQuery"], self.query)
        while result:
            result = result.group(0)
            self.term_query(result)
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["termQuery"], self.query)

#------------------------------------------------------------------------#

    def price_query(self, query):
        # "price[ ]*(<|>|<=|>=|=)[ ]*[0-9]+"
        operator = re.search("(<=|>=|=|<|>)", query).group(0)
        price = re.search("[0-9]+", query).group(0)
        fprice = "{:>12}".format(price)
        if operator == ">=":
            self.query_data["price >="].append(price)
            eprice = bytes(fprice, encoding = "utf-8")
            result = self.priceCursor.set_range(eprice)
            
            while result:
                ad = result[1].decode("utf-8").split(",")
                adID = ad[0]
                adPrice = int(result[0].decode("utf-8"))
                if adPrice >= int(price):
                
                    self.priceMatches.append(adID)
                    #print(adID)
                else:
                    break
                result = self.priceCursor.next()
              
        if operator == "<=":
            self.query_data["price <="].append(price)
            eprice = bytes(fprice, encoding = "utf-8")
            result = self.priceCursor.set_range(eprice)
            
            while result:
                ad = result[1].decode("utf-8").split(",")
                adID = ad[0]
                adPrice = int(result[0].decode("utf-8"))
                if adPrice <= int(price):
                    self.priceMatches.append(adID)
                    #print(adID)
                    
                    #now check for duplicates first
                    dup = self.priceCursor.next_dup()
                    while dup:
                        dupAd = dup[1].decode("utf-8").split(",")
                        dupID = dupAd[0]
                        self.priceMatches.append(dupID)
                        #print(dupID)
                        dup = self.priceCursor.next_dup()
                else:
                    break
                result = self.priceCursor.prev_nodup()            

        if operator == ">":
            self.query_data["price >"].append(price)
            eprice = bytes(fprice, encoding = "utf-8")
            result = self.priceCursor.set_range(eprice)
            result = self.priceCursor.next_nodup()
            while result:
                ad = result[1].decode("utf-8").split(",")
                adID = ad[0]
                adPrice = int(result[0].decode("utf-8"))
                if adPrice > int(price):
                    self.priceMatches.append(adID)
                    #print(adID)
                else:
                    break
                result = self.priceCursor.next()            

        if operator == "<":
            self.query_data["price <"].append(price)
            eprice = bytes(fprice, encoding = "utf-8")
            result = self.priceCursor.set_range(eprice)
            result = self.priceCursor.prev_nodup()
            while result:
                ad = result[1].decode("utf-8").split(",")
                adID = ad[0]
                adPrice = int(result[0].decode("utf-8"))
                if adPrice < int(price):
                    self.priceMatches.append(adID)
                    #print(adID)
                    
                    dup = self.priceCursor.prev_dup()
                    while dup:
                        dupAd = dup[1].decode("utf-8").split(",")
                        dupID = dupAd[0]
                        self.priceMatches.append(dupID)
                        #print(dupID)
                        dup = self.priceCursor.prev_dup()                    
                else:
                    break
                result = self.priceCursor.prev_nodup()            

        if operator == "=":
            self.query_data["price ="].append(price)
            
            eprice = bytes(fprice, encoding = "utf-8")
            result = self.priceCursor.set(eprice)
            
            if result:
                ad = result[1].decode("utf-8").split(",")
                adID = ad[0]
                self.priceMatches.append(adID)
                #print(adID)
                dup = self.priceCursor.next_dup()
                while dup:
                    dupAd = dup[1].decode("utf-8").split(",")
                    dupID = dupAd[0]
                    self.priceMatches.append(dupID)
                    #print(dupID)
                    dup = self.priceCursor.next_dup()
                    

#------------------------------------------------------------------------#

    def date_query(self, query):
        # "date[ ]*(<|>|<=|>=|=)[ ]*[0-9]{4}/[0-9]{2}/[0-9]{2}"
        operator = re.search("(<=|>=|=|<|>)", query).group(0)
        date = re.search("[0-9]{4}/[0-9]{2}/[0-9]{2}", query).group(0)
        
        
        
        if date not in self.reserved_keywords:
            
            #format the date first
            fdate = "{:>1}".format(date)
            date = dt.strptime(date,"%Y/%M/%d")
            edate = bytes(fdate,encoding = "utf-8")
            if operator == ">=":
                self.query_data["date >="].append(date)
                result = self.dateCursor.set_range(edate)
                while result:
                    ad = result[1].decode("utf-8").split(",")
                    adID = ad[0]
                    adDate = result[0].decode("utf-8")
                    adDate = dt.strptime(adDate,"%Y/%M/%d")
                    if adDate >= date :                    
                        self.dateMatches.append(adID)
                        #print(adID)
                    else:
                        break
                    result = self.dateCursor.next()
                    
                    
                
            if operator == "<=":
                self.query_data["date <="].append(date)
                result = self.dateCursor.set_range(edate)
                
                while result:
                    ad = result[1].decode("utf-8").split(",")
                    adID = ad[0]
                    adDate = result[0].decode("utf-8")
                    adDate = dt.strptime(adDate,"%Y/%M/%d")
                    if adDate <= date :
                        self.dateMatches.append(adID)
                        #print(adID)
                        
                        dup = self.dateCursor.next_dup()
                        while dup:
                            dupAd = dup[1].decode("utf-8").split(",")
                            dupID = dupAd[0]
                            self.dateMatches.append(dupID)
                            #print(dupID)
                            dup = self.dateCursor.next_dup()
                    else:
                        break
                    result = self.dateCursor.prev_nodup()                

            if operator == ">":
                self.query_data["date >"].append(date)
                result = self.dateCursor.set_range(edate)
                result = self.dateCursor.next_nodup()
                while result:
                    ad = result[1].decode("utf-8").split(",")
                    adID = ad[0]
                    adDate = result[0].decode("utf-8")
                    adDate = dt.strptime(adDate,"%Y/%M/%d")
                    if adDate > date :                            
                        self.dateMatches.append(adID)
                        #print(adID)
                    else:
                        break
                    result = self.dateCursor.next()                

            if operator == "<":
                self.query_data["date <"].append(date)
                result = self.dateCursor.set_range(edate)
                result = self.dateCursor.prev_nodup()
                while result:
                    ad = result[1].decode("utf-8").split(",")
                    adID = ad[0]
                    adDate = result[0].decode("utf-8")
                    adDate = dt.strptime(adDate,"%Y/%M/%d")
                
                    if adDate < date :
                        self.dateMatches.append(adID)
                        #print(adID)
                        
                        dup = self.dateCursor.prev_dup()
                        while dup:
                            dupAd = dup[1].decode("utf-8").split(",")
                            dupID = dupAd[0]
                            self.dateMatches.append(dupID)
                            #print(dupID)
                            dup = self.dateCursor.prev_dup()
                    else:
                        break
                    result = self.dateCursor.prev_nodup()                 

            if operator == "=":
                self.query_data["date ="].append(date)
                result = self.dateCursor.set_range(edate)
                if result:
                    ad = result[1].decode("utf-8").split(",")
                    adID = ad[0]
                    self.dateMatches.append(adID)
                    #print(adID)
                    dup = self.dateCursor.next_dup()
                    while dup:
                        dupAd = dup[1].decode("utf-8").split(",")
                        dupID = dupAd[0]
                        self.dateMatches.append(dupID)
                        #print(dupID)
                        dup = self.dateCursor.next_dup()                     

#------------------------------------------------------------------------#

    def location_query(self, query):
        # "location[ ]*=[ ]*[0-9a-zA-Z_-]+"
        location = re.search("[0-9a-zA-Z_-]+\Z", query).group(0).lower()
        if location not in self.reserved_keywords:
            self.query_data["locations"].append(location) 
        
            LOC = bytes(location, encoding='utf-8') #encoding for db needs to be 'utf-8'
            #self.LOC_adIDs =[] # add as a self.LOCadIDs ???
            runThrough = self.priceCursor.first() #returns the first item in the ad row
                    
            # we can use either the date or price index [locations are in both]...
            # [*] -- Search Index: Price Index
        
            while runThrough: #runThrough/search the price index 
                # Iterate through all price records (price index db) and find the records/lines that have the locations  
                returnedLOC = self.priceCursor.get(location, db.DB_CURRENT)[1].decode('utf-8')
                returnedLocation = returnedLOC.split(',')[2]
                #print(LOC.decode('utf-8'))

                if returnedLocation.lower() == LOC.decode('utf-8'): # if match append the adID of the LOC (location term queried) to the adID list
                    self.LOC_adIds.append(returnedLOC.split(',')[0]) #
                    #print(returnedLOC.split(',')[0]))
                runThrough = self.priceCursor.next() #increment the cursor
            
            

#------------------------------------------------------------------------#

    def category_query(self, query):
        # "cat[ ]*=[ ]*[0-9a-zA-Z_-]+"
        category = re.search("[0-9a-zA-Z_-]+\Z", query).group(0).lower()
        if category not in self.reserved_keywords:
            self.query_data["categories"].append(category)
            CAT = bytes(category, encoding = 'utf-8')
            #self.CAT_adIds = []
            runThrough = self.priceCursor.first()
    
    
            while runThrough: #runThrough/search the price index 
                # Iterate through all records and find the records where the category matches
                returnedCAT = self.priceCursor.get(CAT, db.DB_CURRENT)[1].decode('utf-8')
                returnedCategory = returnedCAT.split(',')[1]
                #print(CAT.decode('utf-8'))

                if returnedCategory.lower() == CAT.decode('utf-8'):
                    self.CAT_adIds.append(returnedCAT.split(',')[0])

                runThrough = self.priceCursor.next() #increment the cursor

#------------------------------------------------------------------------#
    def term_query(self, query):
        # "([0-9a-zA-Z_-]+%|[0-9a-zA-Z_-]+)"
        term = re.search("[0-9a-zA-Z_-]+", query).group(0).lower()
        if term not in self.reserved_keywords:
            inexact = re.search("%", query)
            if inexact: #partial matching {case for term%}
                inexact = True
                self.query_data["terms%"].append(term)
            
            else: #no partial matching {Nocase for term%}
                inexact = False
                self.query_data["terms"].append(term)

#------------------------------------------------------------------------#

    def print_query_conditions(self):
        pattern = "{:<12}:   {}"
        print(self.original_query)
        print()
        for key in self.query_data.keys():
            print(pattern.format(key, self.query_data[key]))
        print()
        
    
#------------------------------------------------------------------------#
#------------------------------------------------------------------------#

class Interface:
    def __init__(self):

        # By default, the output of each query is the ad id and the title of all matching ads. 
        # The user should be able to change the output format to full record by typing "output=full" and back to id and title only using "output=brief"
        self.brief_output = True
        self.parser = QueryParser()


    # Function to clear the screen - less clutter
    def clear_screen(self):
        os.system("clear")

    
    def main_menu(self):
        self.clear_screen()

        print("> Select a number option from the menu\n")

        print("   1. Terminal (enter queries / change output mode)")
        print("   2. View query grammar")
        print("   3. Exit")

        # Validate input
        valid_choice = False
        
        while not valid_choice:
            choice = input()
            
            if choice == "1":
                self.terminal()

            elif choice == "2":
                self.grammar()

            elif choice == "3":
                self.clear_screen()
                self.parser.close_databases()
                quit()
            
            else:
                print("\n   < {} > is not a valid menu option. Try again.\n".format(choice)) #shows the user what they inputted wrong
                continue

    
    def terminal(self):
        self.clear_screen()

        print("Output=full / output=brief not implemented yet")
        print("Also only works with gramatically correct queries (no error handling)")
        print("> Enter a command:\n")

        query = input()
        self.parser.set_query(query)
        self.clear_screen()
        self.parser.print_query_conditions()

        input("> Press enter to return to main menu.\n")

        self.main_menu()


    def grammar(self):
        self.clear_screen()

        print("Price Query: 'price' whitespace* ('=' | '>' | '<' | '>=' | '<=') whitespace* price")
        print("Ex: price >= 10")
        print()
        print("Date Query: 'date' whitespace* ('=' | '>' | '<' | '>=' | '<=') whitespace* date")
        print("Ex: date = 2018/11/21")
        print()
        print("Location Query: 'location' whitespace* '=' whitespace* location")
        print("Ex: location = Edmonton")
        print()
        print("Category Query: 'cat' whitespace* '=' whitespace* category")
        print("Ex: cat = automobile-parts")
        print()
        print("Term Query: term | term '%'")
        print("Ex: camera (exact match)")
        print("Ex: camera% (terms starting with camera)")
        print()
        print("output=brief shows only ad id and title")
        print("output=full shows full record")
        print()

        input("> Press enter to return to main menu.\n")

        self.main_menu()

#-------------------------------------------------------------------------#

interface = Interface()
interface.main_menu()
