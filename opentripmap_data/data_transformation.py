# FRQ-04 - Add a new dimension kinds_amount, which is the count of kinds of a particular place
def calculate_amount_of_kinds(row):
    return len(row["kinds"].split(","))


def add_kinds_amount_column(locations_df):
    locations_df["kinds_amount"] = locations_df.apply(
        lambda row: calculate_amount_of_kinds(row), axis=1
    )
    return locations_df


# FRQ-03 - Filter those records that include the word "skyscrapers" within its kinds
def filter_by_skyscrappers_accommodations(locations_df):
    return locations_df[locations_df["kinds"].str.contains("skyscrapers")]


# FRQ-05 - For every record in the dataframe add dimensions extracted from OpenTripMap details API information
def enrich_locations_dataframe_with_details(locations_df, details_df):
    return locations_df.merge(details_df, on=["xid"], how="inner")
