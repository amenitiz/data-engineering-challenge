from utils import load_json, check_path, save_csv, generate_open_street_map
from opentripcrawler import OpenTripCrawler


c = OpenTripCrawler(load_json(check_path("config.json")))

c.run()

accomodations_df = c.get_accomodations_df()

filename = "places_output.csv"

save_csv(accomodations_df, filename)

print(f"File saved in {filename}")

generate_open_street_map(accomodations_df, "barcelona_map.jpg")
