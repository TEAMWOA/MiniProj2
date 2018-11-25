# Location Query 
# iterate through the locations entered (list form)
# 

def locationQuery(queryList):
    # NOTE*** queryString is of the format location=[A-Za-z0-9]+
    
    #while there are location queries in the list left to search in the indexes for.....
    while len(queryList) != 0:
        for location in queryList: # loop through all locations in the list 
        
        
            #queryString = queryString.lower()
            #location = queryString.split('=')[1]
            location = bytes(location, encoding='utf-8') #encoding for db needs to be 'utf-8'

            # we can use either the date or price index [locations are in both]...
            # 1. --Choose: Price Index
            priceIndex = db.DB() #create index database
            priceIndex.open("pr.idx")
            priceCursor = priceIndex.cursor()
            
            # 2. --Cross reference with: Ad Index - grab the ad id (key)
            adIndex = db.DB() # create ad database
            adIndex.open("ad.idx")
            adCursor = adIndex.cursor()

            adIds = []
            runThrough = priceCursor.first() #returns the first item in the row   

            fullRecords = []
            
            while runThrough:
                # Iterate through all price records (price index db) and find the records/lines that have the locations
                
                returnedValue = priceCursor.get(location, db.DB_CURRENT)[1].decode('utf-8')
                returnedLocation = returnedValue.split(',')[2]
                print(location.decode('utf-8'))

                if returnedLocation.lower() == location.decode('utf-8'):
                    adIds.append(returnedValue.split(',')[0]) #

                runThrough = priceCursor.next()


    if len(queryList) == 0:
        adIds = []
    
    return adIds