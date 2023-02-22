from utils import load_json, check_path, save_csv
from opentripcrawler import OpenTripCrawler


c = OpenTripCrawler(load_json(check_path("config.json")))

c.run()

accomodations_df = c.get_accomodations_df()

accomodations_df.show()

save_csv(accomodations_df, "places_output.csv")

print("saved")