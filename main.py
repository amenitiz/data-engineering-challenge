from utils import load_json, check_path
from opentripcrawler import OpenTripCrawler

c = OpenTripCrawler(load_json(check_path("config.json")))
