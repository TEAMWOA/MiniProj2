def locationQuery(queryString):
    # queryString is of the format location=[A-Za-z0-9]+
    # TODO: format the string so you can conduct queries on it
    queryString = queryString.lower()
    location = queryString.split('=')[1]
    location = bytes(location, encoding='utf-8')

    # we can use either the date or price index
    priceIndex = db.DB()
    priceIndex.open("pr.idx")
    priceCursor = priceIndex.cursor()
    adIndex = db.DB()
    adIndex.open("ad.idx")
    adCursor = adIndex.cursor()

    adIds = []
    iter = priceCursor.first()

    fullRecords = []
    while iter:
        # Iterate through all records and find the records where the location matches
        returnedValue = priceCursor.get(location, db.DB_CURRENT)[1].decode('utf-8')
        returnedLocation = returnedValue.split(',')[2]
        print(location.decode('utf-8'))

        if returnedLocation.lower() == location.decode('utf-8'):
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