# FRQ-06 - Store it in a CSV file
def store_dataframe_as_csv_file(dataframe, output_file):
    dataframe.to_csv(output_file, sep=";", header=True, index=False)


# FRQ-07 - plot into a JPG file
def plot_dataframe_as_jpg(dataframe, output_file):
    # TODO
    pass
