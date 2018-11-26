import os
import re
from bsddb3 import db
from datetime import datetime as dt


# Function to clear the screen - less clutter
def clear_screen():
    os.system("clear")


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
            "termQuery"      : "([0-9a-zA-Z_-]+%|[0-9a-zA-Z_-]+)",
            "outputBrief"    : "output[ ]*=[ ]*brief",
            "outputFull"     : "output[ ]*=[ ]*full"
        }

        self.reserved_keywords = ["price", "cat", "location", "date", "output"]

        self.output = "brief"
        

        

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

        self.result_sets = []
        self.results = set()

        time1 = dt.now()

        self.parse()

        if not self.optimize_query():
            clear_screen()
            print()
            print("No results\n")

        else:
            clear_screen()
            self.print_query_conditions()

            self.price_query()
            self.date_query()
            self.location_query()
            self.category_query()
            self.term_query()

            if len(self.result_sets) > 1:
                self.results = self.result_sets[0]
                for result_set in self.result_sets:
                    self.results = self.results & result_set

            elif len(self.result_sets) == 1:
                self.results = self.result_sets[0]

            self.time_delta = dt.now() - time1

            self.print_results()


    def print_results(self):

        self.results_dict = {}

        self.adCursor.first()

        for adID in self.results:

            # Iterate through all records and find the records where the category matches
            returned = self.adCursor.set(adID.encode("utf-8"))
            rawAD = self.adCursor.get(adID.encode("utf-8"), db.DB_CURRENT)[1].decode("utf-8")
            ad_data = self.process_ad(rawAD)
            self.results_dict[ad_data["aid"]] = ad_data

        print_format = "{:<11} : {}"

        if self.output == "brief":
            for key in self.results_dict.keys():
                print(print_format.format(key, self.results_dict[key]["ti"]))
                # print()
        else:
            for key in self.results_dict.keys():
                print(print_format.format("Ad ID", self.results_dict[key]["aid"]))
                print(print_format.format("Title", self.results_dict[key]["ti"]))
                print(print_format.format("Date", self.results_dict[key]["date"]))
                print(print_format.format("Location", self.results_dict[key]["loc"]))
                print(print_format.format("Category", self.results_dict[key]["cat"]))
                print(print_format.format("Price", self.results_dict[key]["price"]))
                print(print_format.format("Description", self.results_dict[key]["desc"]))
                print()

        print("\n{} results in {} seconds\n".format(len(self.results), self.time_delta.total_seconds()))


    def process_ad(self, raw_ad):

        # Dictionary containing data about the ad
        ad_data = {
            "aid"   : None,
            "date"  : None,
            "loc"   : None,
            "cat"   : None,
            "ti"    : None,
            "desc"  : None,
            "price" : None
        }

        # Regex pattern
        pattern = "<{}>(.*)</{}>"

        # Extract data from each tag and add it to ad_data
        for key in ad_data.keys():
            regex = pattern.format(key, key)
            result = re.search(regex, raw_ad)
            ad_data[key] = result.group(1)

        # Add raw record string to ad_data (used for ads.txt)
        ad_data["raw_ad"] = raw_ad

        return ad_data


    def parse(self):

        self.query = self.query.lower()

        match = re.search(self.regexes["outputBrief"], self.query)
        if match:
            self.output = "brief"
            print("Output mode set to brief")
            return

        match = re.search(self.regexes["outputFull"], self.query)
        if match:
            self.output = "full"
            print("Output mode set to full")
            return
        
        # Search for price queries
        result = re.search(self.regexes["priceQuery"], self.query)
        while result:
            result = result.group(0)
            
            operator = re.search("(<=|>=|=|<|>)", result).group(0)
            price = int(re.search("[0-9]+", result).group(0))

            if operator == ">=" and price not in self.query_data["price >="]:
                self.query_data["price >="].append(price)
            elif operator == "<=" and price not in self.query_data["price <="]:
                self.query_data["price <="].append(price)
            elif operator == ">" and price not in self.query_data["price >"]:
                self.query_data["price >"].append(price)
            elif operator == "<" and price not in self.query_data["price <"]:
                self.query_data["price <"].append(price)
            elif operator == "=" and price not in self.query_data["price ="]:
                self.query_data["price ="].append(price)

            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["priceQuery"], self.query)

        # Search for date queries
        result = re.search(self.regexes["dateQuery"], self.query)
        while result:
            result = result.group(0)
            operator = re.search("(<=|>=|=|<|>)", result).group(0)
            date = re.search("[0-9]{4}/[0-9]{2}/[0-9]{2}", result).group(0)

            if operator == ">=" and date not in self.query_data["date >="]:
                self.query_data["date >="].append(date)
            elif operator == "<=" and date not in self.query_data["date <="]:
                self.query_data["date <="].append(date)
            elif operator == ">" and date not in self.query_data["date >"]:
                self.query_data["date >"].append(date)
            elif operator == "<" and date not in self.query_data["date <"]:
                self.query_data["date <"].append(date)
            elif operator == "=" and date not in self.query_data["date ="]:
                self.query_data["date ="].append(date)

            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["dateQuery"], self.query)

        # Search for location queries
        result = re.search(self.regexes["locationQuery"], self.query)
        while result:
            result = result.group(0)
            
            location = re.search("[0-9a-zA-Z_-]+\Z", result).group(0).lower()
            if location not in self.reserved_keywords and location not in self.query_data["locations"]:
                self.query_data["locations"].append(location)

            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["locationQuery"], self.query)

        # Search for category queries
        result = re.search(self.regexes["catQuery"], self.query)
        while result:
            result = result.group(0)
            
            category = re.search("[0-9a-zA-Z_-]+\Z", result).group(0).lower()
            if category not in self.reserved_keywords and category not in self.query_data["categories"]:
                self.query_data["categories"].append(category)

            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["catQuery"], self.query)

        # Search for term queries
        result = re.search(self.regexes["termQuery"], self.query)
        while result:
            result = result.group(0)
            
            term = re.search("[0-9a-zA-Z_-]+", result).group(0).lower()
            if term not in self.reserved_keywords:
                inexact = re.search("%", result)
                if inexact and term not in self.query_data["terms%"]:
                    inexact = True
                    self.query_data["terms%"].append(term)
                elif term not in self.query_data["terms"]:
                    inexact = False
                    self.query_data["terms"].append(term)

            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["termQuery"], self.query)


    def optimize_query(self):

        if len(self.query_data["date ="]) >= 2:
            print("No results")
            return False

        if len(self.query_data["price ="]) >= 2:
            print("No results")
            return False

        if len(self.query_data["locations"]) >= 2:
            print("No results")
            return False

        if len(self.query_data["categories"]) >= 2:
            print("No results")
            return False

        if len(self.query_data["price ="]) >= 1:
            self.query_data["price >="] = self.query_data["price <="] = self.query_data["price >"] = self.query_data["price <"] = []

        if len(self.query_data["date ="]) >= 1:
            self.query_data["date >="] = self.query_data["date <="] = self.query_data["date >"] = self.query_data["date <"] = []

        while len(self.query_data["price <"]) > 1:
            self.query_data["price <"].remove(max(self.query_data["price <"]))

        while len(self.query_data["price <="]) > 1:
            self.query_data["price <="].remove(max(self.query_data["price <="]))

        while len(self.query_data["price >"]) > 1:
            self.query_data["price >"].remove(min(self.query_data["price >"]))

        while len(self.query_data["price >="]) > 1:
            self.query_data["price >="].remove(min(self.query_data["price >="]))

        for price1 in self.query_data["price <"]:
            for price2 in self.query_data["price >"]:
                if price1 <= (price2 + 1):
                    print("No results")
                    return False

        for price1 in self.query_data["price <="]:
            for price2 in self.query_data["price >"]:
                if price1 <= price2:
                    print("No results")
                    return False

        for price1 in self.query_data["price <="]:
            for price2 in self.query_data["price >="]:
                if price1 < price2:
                    print("No results")
                    return False

        for price1 in self.query_data["price <"]:
            for price2 in self.query_data["price >="]:
                if price1 <= price2:
                    print("No results")
                    return False

        while len(self.query_data["date <"]) > 1:
            dates = []
            for sdate in self.query_data["date <"]:
                dates.append(dt.strptime(sdate, "%Y/%m/%d"))
            self.query_data["date <"].remove(dt.strftime(max(dates), "%Y/%m/%d"))

        while len(self.query_data["date <="]) > 1:
            dates = []
            for sdate in self.query_data["date <="]:
                dates.append(dt.strptime(sdate, "%Y/%m/%d"))
            self.query_data["date <="].remove(dt.strftime(max(dates), "%Y/%m/%d"))

        while len(self.query_data["date >"]) > 1:
            dates = []
            for sdate in self.query_data["date >"]:
                dates.append(dt.strptime(sdate, "%Y/%m/%d"))
            self.query_data["date >"].remove(dt.strftime(min(dates), "%Y/%m/%d"))

        while len(self.query_data["date >="]) > 1:
            dates = []
            for sdate in self.query_data["date >="]:
                dates.append(dt.strptime(sdate, "%Y/%m/%d"))
            self.query_data["date >="].remove(dt.strftime(min(dates), "%Y/%m/%d"))

        for sdate1 in self.query_data["date <"]:
            for sdate2 in self.query_data["date >"]:
                date1 = dt.strptime(sdate1, "%Y/%m/%d")
                date2 = dt.strptime(sdate2, "%Y/%m/%d")
                if date1 <= date2:
                    print("No results")
                    return False

        for sdate1 in self.query_data["date <="]:
            for sdate2 in self.query_data["date >"]:
                date1 = dt.strptime(sdate1, "%Y/%m/%d")
                date2 = dt.strptime(sdate2, "%Y/%m/%d")
                if date1 <= date2:
                    print("No results")
                    return False

        for sdate1 in self.query_data["date <="]:
            for sdate2 in self.query_data["date >="]:
                date1 = dt.strptime(sdate1, "%Y/%m/%d")
                date2 = dt.strptime(sdate2, "%Y/%m/%d")
                if date1 < date2:
                    print("No results")
                    return False

        for sdate1 in self.query_data["date <"]:
            for sdate2 in self.query_data["date >="]:
                date1 = dt.strptime(sdate1, "%Y/%m/%d")
                date2 = dt.strptime(sdate2, "%Y/%m/%d")
                if date1 <= date2:
                    print("No results")
                    return False

        return True


    # def location_query(self):
    def location_query(self):
        for location in self.query_data["locations"]:
            # print(location)
            results = set()
        
            LOC = bytes(location, encoding='utf-8') #encoding for db needs to be 'utf-8'
            #self.LOC_adIDs =[] # add as a self.LOCadIDs ???
            runThrough = self.priceCursor.first() #returns the first item in the ad row
                    
            # we can use either the date or price index [locations are in both]...
            # [*] -- Search Index: Price Index
        
            while runThrough: #runThrough/search the price index 
                # Iterate through all price records (price index db) and find the records/lines that have the locations  
                returnedLOC = self.priceCursor.get(location.encode("utf-8"), db.DB_CURRENT)[1].decode('utf-8')
                returnedLocation = returnedLOC.split(',')[2]
                #print(LOC.decode('utf-8'))

                if returnedLocation.lower() == location: # if match append the adID of the LOC (location term queried) to the adID list
                    results.add(returnedLOC.split(',')[0]) #
                    #print(returnedLOC.split(',')[0]))
                runThrough = self.priceCursor.next() #increment the cursor

            self.result_sets.append(results)


    def category_query(self):
        # "cat[ ]*=[ ]*[0-9a-zA-Z_-]+"
        for category in self.query_data["categories"]:
            results = set()
            CAT = bytes(category, encoding = 'utf-8')
            runThrough = self.priceCursor.first()
            
            # [*] -- Search Index: Price Index
    
            while runThrough: #runThrough/search the price index 
                # Iterate through all records and find the records where the category matches
                returnedCAT = self.priceCursor.get(CAT, db.DB_CURRENT)[1].decode('utf-8')
                returnedCategory = returnedCAT.split(',')[1]
                #print(CAT.decode('utf-8'))

                if returnedCategory.lower() == CAT.decode('utf-8'):
                    results.add(returnedCAT.split(',')[0])

                runThrough = self.priceCursor.next() #increment the cursor

            
            self.result_sets.append(results)


    def term_query(self):

        #{Case A} partial matching {case for 'term%'}
        for term in self.query_data["terms%"]:

            results = set()
			
            self.termCursor.first()

            current = self.termCursor.set_range(term.encode("utf-8"))
            returnedTERM = self.termCursor.get(db.DB_CURRENT)[0].decode("utf-8")
            returnedID = self.termCursor.get(db.DB_CURRENT)[1].decode("utf-8")
            
                
            while returnedTERM.startswith(term):

                results.add(returnedID)

                current = self.termCursor.next()

                returnedTERM = self.termCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                returnedID = self.termCursor.get(db.DB_CURRENT)[1].decode("utf-8")
                
            self.result_sets.append(results)
                            
        # {Case B} no partial matching {Nocase for 'term%' aka 'term'}
        for term in self.query_data["terms"]:

            results = set()

            self.termCursor.first()
			
            current = self.termCursor.set(term.encode("utf-8"))
            returnedTERM = self.termCursor.get(db.DB_CURRENT)[0].decode("utf-8")
            returnedID = self.termCursor.get(db.DB_CURRENT)[1].decode("utf-8")

            while returnedTERM == term:

                results.add(returnedID)

                current = self.termCursor.next()

                returnedTERM = self.termCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                returnedID = self.termCursor.get(db.DB_CURRENT)[1].decode("utf-8")
                
            self.result_sets.append(results)

        
    def date_query(self):

        for date1 in self.query_data["date >="]:

            results = set()

            dtdate1 = dt.strptime(date1, "%Y/%m/%d")

            current = self.dateCursor.first()
            while current:
                date2 = self.dateCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                adID = self.dateCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]

                dtdate2 = dt.strptime(date2, "%Y/%m/%d")
    
                if dtdate2 >= dtdate1:
                    results.add(adID)

                current = self.dateCursor.next()

            self.result_sets.append(results)

        for date1 in self.query_data["date <="]:

            results = set()

            dtdate1 = dt.strptime(date1, "%Y/%m/%d")

            current = self.dateCursor.first()
            while current:
                date2 = self.dateCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                adID = self.dateCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]

                dtdate2 = dt.strptime(date2, "%Y/%m/%d")
    
                if dtdate2 <= dtdate1:
                    results.add(adID) 

                current = self.dateCursor.next()

            self.result_sets.append(results)

        for date1 in self.query_data["date >"]:

            results = set()

            dtdate1 = dt.strptime(date1, "%Y/%m/%d")

            current = self.dateCursor.first()
            while current:
                date2 = self.dateCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                adID = self.dateCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]

                dtdate2 = dt.strptime(date2, "%Y/%m/%d")
    
                if dtdate2 > dtdate1:
                    results.add(adID)

                current = self.dateCursor.next()

            self.result_sets.append(results)

        for date1 in self.query_data["date <"]:

            results = set()

            dtdate1 = dt.strptime(date1, "%Y/%m/%d")

            current = self.dateCursor.first()
            while current:
                date2 = self.dateCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                adID = self.dateCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]

                dtdate2 = dt.strptime(date2, "%Y/%m/%d")
    
                if dtdate2 < dtdate1:
                    results.add(adID)

                current = self.dateCursor.next()

            self.result_sets.append(results)

        for date1 in self.query_data["date ="]:

            results = set()

            dtdate1 = dt.strptime(date1, "%Y/%m/%d")

            current = self.dateCursor.first()
            while current:
                date2 = self.dateCursor.get(db.DB_CURRENT)[0].decode("utf-8")
                adID = self.dateCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]

                dtdate2 = dt.strptime(date2, "%Y/%m/%d")
    
                if dtdate2 == dtdate1:
                    results.add(adID)

                current = self.dateCursor.next()

            self.result_sets.append(results)


    def price_query(self):

        for price1 in self.query_data["price >="]:

            results = set()

            current = self.priceCursor.first()
            while current:
                price2 = int(self.priceCursor.get(db.DB_CURRENT)[0].decode("utf-8"))
                adID = self.priceCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]
    
                if price2 >= price1:
                    results.add(adID)

                current = self.priceCursor.next()

            self.result_sets.append(results)

        for price1 in self.query_data["price <="]:

            results = set()

            current = self.priceCursor.first()
            while current:
                price2 = int(self.priceCursor.get(db.DB_CURRENT)[0].decode("utf-8"))
                adID = self.priceCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]
    
                if price2 <= price1:
                    results.add(adID)

                current = self.priceCursor.next()

            self.result_sets.append(results)

        for price1 in self.query_data["price >"]:

            results = set()

            current = self.priceCursor.first()
            while current:
                price2 = int(self.priceCursor.get(db.DB_CURRENT)[0].decode("utf-8"))
                adID = self.priceCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]
    
                if price2 > price1:
                    results.add(adID)

                current = self.priceCursor.next()

            self.result_sets.append(results)

        for price1 in self.query_data["price <"]:

            results = set()

            current = self.priceCursor.first()
            while current:
                price2 = int(self.priceCursor.get(db.DB_CURRENT)[0].decode("utf-8"))
                adID = self.priceCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]
    
                if price2 < price1:
                    results.add(adID)

                current = self.priceCursor.next()

            self.result_sets.append(results)

        for price1 in self.query_data["price ="]:

            results = set()

            current = self.priceCursor.first()
            while current:
                price2 = int(self.priceCursor.get(db.DB_CURRENT)[0].decode("utf-8"))
                adID = self.priceCursor.get(db.DB_CURRENT)[1].decode("utf-8").split(",")[0]
    
                if price2 == price1:
                    results.add(adID)

                current = self.priceCursor.next()

            self.result_sets.append(results)


    def print_query_conditions(self):
        pattern = "{:<12}:   {}"
        print(self.original_query)
        print("Output Mode: {}".format(self.output))
        print()
        for key in self.query_data.keys():
            print(pattern.format(key, self.query_data[key]))
        print()

#------------------------------------------------------------------------#

class Interface:
    def __init__(self):

        # By default, the output of each query is the ad id and the title of all matching ads. 
        # The user should be able to change the output format to full record by typing "output=full" and back to id and title only using "output=brief"
        self.parser = QueryParser()

    
    def main_menu(self):
        clear_screen()

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
                clear_screen()
                self.parser.close_databases()
                quit()
            
            else:
                print("\n   < {} > is not a valid menu option. Try again.\n".format(choice)) #shows the user what they inputted wrong
                continue

    
    def terminal(self):
        clear_screen()

        print("> Enter a command:\n")

        query = input()
        self.parser.set_query(query)

        input("> Press enter to return to main menu.\n")

        self.main_menu()


    def grammar(self):
        clear_screen()

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
