input_directory = "XMLFiles/{}/{}"
    output_directory = "TextFiles/{}/{}"

    # 10, 1k, 20k, 100k
    dataset = "10"

    parsed_ads = 0  # Initialize count for number of parsed ads
    start_time = datetime.now()  # Start timer for recording runtime

    # Open files
    xml_file = open(input_directory.format(dataset, "{}.txt".format(dataset)), "r")  # XML File to read from
    terms_file = open(output_directory.form