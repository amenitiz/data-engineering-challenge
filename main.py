from utils import load_json, check_path
from opentripcrawler import OpenTripCrawler

c = OpenTripCrawler(load_json(check_path("config.json")))

accomodations_df = c.get_accomodations_df()

kinds_contain_skyscrapers = c.filter_by_col_keywords(accomodations_df, "kinds", "skyscrapers")