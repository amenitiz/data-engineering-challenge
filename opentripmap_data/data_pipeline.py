from datetime import datetime

from data_extraction import create_location_details_df
from data_extraction import extract_locations_data_from_opentripmap_to_df
from data_loading import store_dataframe_as_csv_file
from data_transformation import add_kinds_amount_column
from data_transformation import enrich_locations_dataframe_with_details
from data_transformation import filter_by_skyscrappers_accommodations

TODAY_DATE = datetime.strftime(datetime.now(), "%Y-%m-%d")
OUTPUT_FILE_PATH = "output_files"
OUTPUT_FILE_NAME = f"skyscrappers_bcn_{TODAY_DATE}.csv"

# Extract data and store it in a Pandas DataFrame
print("Extracting Locations data from Open Trip Map API")
locations_df = extract_locations_data_from_opentripmap_to_df()
print("Data extraction ended")

# Add a new column with the number of kinds each accommodation has
print("Data Transformation started")
locations_df = add_kinds_amount_column(locations_df)

# Filter by skyscrapper accommodation
skyscrapper_df = filter_by_skyscrappers_accommodations(locations_df)

print("Extracting Location Details data from Open Trip Map API")
skyscrapper_details_df = create_location_details_df(skyscrapper_df)

skyscrapper_df = enrich_locations_dataframe_with_details(
    skyscrapper_df, skyscrapper_details_df
)

print("Data Transformation ended")

print(f"Storing skyscrapper data in {OUTPUT_FILE_PATH}/{OUTPUT_FILE_NAME}")
# Store dataframe as CSV file
store_dataframe_as_csv_file(
    dataframe=skyscrapper_df, output_file=f"{OUTPUT_FILE_PATH}/{OUTPUT_FILE_NAME}"
)
print(f"File {OUTPUT_FILE_PATH}/{OUTPUT_FILE_NAME} created successfully")
