lon_min = 2.028471
lon_max = 2.283903
lat_min = 41.315758
lat_max = 41.451768

base_url = 'https://api.opentripmap.com/0.1/en/places/'
api_key = '5ae2e3f221c38a28845f05b656934aef7d36d2c813d4ee1bc9d86b1d'

# keys in json to delete
keys = ['address', 'rate', 'osm', 'bbox', 'wikidata', 'sources', 'otm', 'preview', 'wikipedia_extracts', 'point']

# dic schema for detailed properties
dic = {'xid': [],
       'name': [],
       'kinds': [],
       'url': [],
       'stars': [],
       'wikipedia': [],
       'image': [],
       'address': []}

schema = ['xid', 'name', 'address', 'kinds', 'kinds_amount', 'stars', 'lon', 'lat', 'url', 'wikipedia', 'image']
