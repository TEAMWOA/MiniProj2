import re
from datetime import datetime


# Extracts terms from the ad's title and description and writes them to terms.txt
# E.g. nikon:1304786670
def terms_function(terms_file, ad_data):

    text_fields = [ad_data["ti"], ad_data["desc"]]  # List containing ad title and description
    keywords = []  # List containing all valid terms / keywords

    # Regular Expressions
    sub_pattern1 = "&#.{0,5};"
    sub_pattern2 = "&.{0,5};"
    keyword_pattern = "[0-9a-zA-Z_-]{3,}"

    # Parse text
    for text in text_fields:
        text = re.sub(sub_pattern1, "", text)  # Removes numeric special characters from the text field (e.g. &#039;)
        text = re.sub(sub_pattern2, " ", text)  # Replaces non-numeric special characters with spaces (e.g. &amp;)
        valid_words = re.findall(keyword_pattern, text)  # Finds all valid words in the text field (length > 2, a-z, A-Z, 0-9, -, _)
        
        # Add valid words to list of keywords (lowercase)
        for word in valid_words:
            keywords.append(word.lower())

    # Write all keywords to terms.txt
    write_pattern = "{}:{}\n"
    for keyword in keywords:
        terms_file.write(write_pattern.format(keyword, ad_data["aid"]))


# Writes posting date information to pdates.txt
# E.g. 2018/11/07:1304786670,camera-camcorder-lens,Calgary
def pdates_function(pdates_file, ad_data):

    date_string = "{}:{},{},{}\n"
    formatted = date_string.format(ad_data["date"], ad_data["aid"], ad_data["cat"], ad_data["loc"])
    pdates_file.write(formatted)


# Writes price information tp prices.txt
# E.g. 8500:1304786670,camera-camcorder-lens,Calgary
def prices_function(prices_file, ad_data):

    price_string = "{:>12}:{},{},{}\n"
    formatted = price_string.format(ad_data["price"], ad_data["aid"], ad_data["cat"], ad_data["loc"])
    prices_file.write(formatted)


# Writes ad information to ads.txt
# E.g. 1304786670:<ad><aid>1304786670</aid><date>2018/11/07</date><loc>Calgary</loc><cat>camera-camcorder-lens</cat><ti>Nikon 500 mm F4 VR</ti><desc>I have owned this Nikon lens for about 2 years and purchased it new in Calgary. The lens is extremely sharp, and fast focusing. It is a wildlife or bird photographers dream lens. I am selling it</desc><price>8500</price></ad>
def ads_function(ads_file, ad_data):

    ad_string = "{}:{}\n"
    formatted = ad_string.format(ad_data["aid"], ad_data["raw_ad"])
    ads_file.write(formatted)


# Extract ad data from the raw record string
# Returns dictionary containing data about the ad
def process_ad(raw_ad):

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


def main():

    input_directory = "XML/{}"
    output_directory = "Output/{}"

    parsed_ads = 0  # Initialize count for number of parsed ads
    start_time = datetime.now()  # Start timer for recording runtime

    # Open files
    xml_file = open(input_directory.format("1k.txt"), "r")  # XML File to read from
    terms_file = open(output_directory.format("terms.txt"), "w")  # Terms file
    pdates_file = open(output_directory.format("pdates.txt"), "w")  # Posting date file
    prices_file = open(output_directory.format("prices.txt"), "w")  # Price file
    ads_file = open(output_directory.format("ads.txt"), "w")  # Ads file

    # xml_file = open("XML/1k.txt", "r")  # XML File to read from
    # terms_file = open("Output/terms.txt", "w")  # Terms file
    # pdates_file = open("Output/pdates.txt", "w")  # Posting date file
    # prices_file = open("Output/prices.txt", "w")  # Price file
    # ads_file = open("Output/ads.txt", "w")  # Ads file

    # Regex pattern
    pattern = "<{}>(.*)</{}>"
    ad_pattern = pattern.format("ad", "ad")

    # Iterate through each line in the XML file, parsing / processing it if it's an ad
    for line in xml_file:
        ad = re.search(ad_pattern, line)
        if ad:
            record = ad.group(0)  # Raw record string
            ad_data = process_ad(record)  # Extract ad data from the record
    
            # Pass ad data to the functions for each file
            terms_function(terms_file, ad_data)
            pdates_function(pdates_file, ad_data)
            prices_function(prices_file, ad_data)
            ads_function(ads_file, ad_data)

            parsed_ads += 1  # Increment number of ads parsed

    # Close files
    xml_file.close()
    terms_file.close()
    pdates_file.close()
    prices_file.close()
    ads_file.close()

    # End timer
    time_delta = datetime.now() - start_time

    # Print program info
    info_string = "Parsed {} ads in {} seconds."
    print(info_string.format(parsed_ads, (int(time_delta.microseconds) / 1000000)))


# Run program
if __name__ == "__main__":
    main()