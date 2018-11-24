import os
import re


class QueryParser:
    def __init__(self, query):

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

        self.original_query = query
        self.query = query


    def parse(self):
        
        # Search for price queries
        result = re.search(self.regexes["priceQuery"], self.query)
        while result:
            result = result.group(0)
            self.price_query(result)
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["priceQuery"], self.query)

        # Search for date queries
        result = re.search(self.regexes["dateQuery"], self.query)
        while result:
            result = result.group(0)
            self.date_query(result)
            self.query = self.query.replace(result, "", 1)
            result = re.search(self.regexes["dateQuery"], self.query)

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


    def price_query(self, query):
        # "price[ ]*(<|>|<=|>=|=)[ ]*[0-9]+"
        operator = re.search("(<=|>=|=|<|>)", query).group(0)
        price = re.search("[0-9]+", query).group(0)

        if operator == ">=":
            self.query_data["price >="].append(price)

        if operator == "<=":
            self.query_data["price <="].append(price)

        if operator == ">":
            self.query_data["price >"].append(price)

        if operator == "<":
            self.query_data["price <"].append(price)

        if operator == "=":
            self.query_data["price ="].append(price)


    def date_query(self, query):
        # "date[ ]*(<|>|<=|>=|=)[ ]*[0-9]{4}/[0-9]{2}/[0-9]{2}"
        operator = re.search("(<=|>=|=|<|>)", query).group(0)
        date = re.search("[0-9]{4}/[0-9]{2}/[0-9]{2}", query).group(0)

        if date not in self.reserved_keywords:
            if operator == ">=":
                self.query_data["date >="].append(date)

            if operator == "<=":
                self.query_data["date <="].append(date)

            if operator == ">":
                self.query_data["date >"].append(date)

            if operator == "<":
                self.query_data["date <"].append(date)

            if operator == "=":
                self.query_data["date ="].append(date)


    def location_query(self, query):
        # "location[ ]*=[ ]*[0-9a-zA-Z_-]+"
        location = re.search("[0-9a-zA-Z_-]+\Z", query).group(0).lower()
        if location not in self.reserved_keywords:
            self.query_data["locations"].append(location)


    def category_query(self, query):
        # "cat[ ]*=[ ]*[0-9a-zA-Z_-]+"
        category = re.search("[0-9a-zA-Z_-]+\Z", query).group(0).lower()
        if category not in self.reserved_keywords:
            self.query_data["categories"].append(category)


    def term_query(self, query):
        # "([0-9a-zA-Z_-]+%|[0-9a-zA-Z_-]+)"
        term = re.search("[0-9a-zA-Z_-]+", query).group(0).lower()
        if term not in self.reserved_keywords:
            inexact = re.search("%", query)
            if inexact:
                inexact = True
                self.query_data["terms%"].append(term)
            else:
                inexact = False
                self.query_data["terms"].append(term)


    def print_query_conditions(self):
        pattern = "{:<12}:   {}"
        print(self.original_query)
        print()
        for key in self.query_data.keys():
            print(pattern.format(key, self.query_data[key]))
        print()

#------------------------------------------------------------------------#

class Interface:
    def __init__(self):

        # By default, the output of each query is the ad id and the title of all matching ads. 
        # The user should be able to change the output format to full record by typing "output=full" and back to id and title only using "output=brief"
        self.brief_output = True


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
                quit()
            
            else:
                print("\n   < {} > is not a valid menu option. Try again.\n".format(choice)) #shows the user what they inputted wrong
                continue

    
    def terminal(self):
        self.clear_screen()

        print("Output=full / output=brief not implemented yet")
        print("Also only works with gramatically correct queries (no error handling)")
        print("> Enter a command:\n")

        command = input()
        parser = QueryParser(command)
        parser.parse()
        self.clear_screen()
        parser.print_query_conditions()

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