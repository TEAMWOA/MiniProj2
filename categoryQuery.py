def categoryQuery(queryString):
    queryString = queryString.lower()
    ##takes item after '=' to be the searched for category and then removes whitespace
    category = (queryString.split("=")[1]).strip()
    category = bytes(category, encoding = 'utf-8')

    priceIndex=db.DB()
    priceIndex.open("pr.idx")
    priceCursor = priceIndex.cursor()
    adIndex = db.DB()
    adIndex.open("ad.idx")
    adCursor = adIndex.cursor()

    # all values that match the desired key
    adIds = []
    iter = priceCursor.first()
    fullRecords = []

    while iter:
        # Iterate through all records and find the records where the location matches
        returnedValue = priceCursor.get(category, db.DB_CURRENT)[1].decode('utf-8')
        returnedLocation = returnedValue.split(',')[1]
        #print(category.decode('utf-8'))

        if returnedLocation.lower() == category.decode('utf-8'):
            adIds.append(returnedValue.split(',')[0])

        iter = priceCursor.next()

    # Now that we have the adIds we can get their titles from ad.idx
    for adId in adIds:
        # we are guaranteed to find the ads in ad.idx
        adId = bytes(adId, encoding='utf-8')
        adCursor.set(adId)
        record = adCursor.get(adId, db.DB_CURRENT)

        # now that we have the record we can return the ID as well as the ad record
        fullRecords.append((adId.decode('utf-8'), record[1].decode('utf-8')))

    return fullRecords