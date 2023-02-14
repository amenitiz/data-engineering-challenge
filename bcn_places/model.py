import requests
import pandas as pd
from global_ import api_key, keys, dic, schema, base_url, lon_max, lon_min, lat_max, lat_min
from data import Prepare, get_schema


class Model:
    def __init__(self):
        self.url = f"{base_url}bbox?lon_min={lon_min}&lon_max={lon_max}&lat_min={lat_min}&lat_max={lat_max}&kinds" \
                   f"=accomodations&format=json&"
        self.key = api_key

    def get_objects(self):
        request = f"{self.url}&apikey={self.key}"
        objects = pd.read_json(request)

        return objects

    def get_df_properties(self):
        data = Prepare(self.get_objects())
        return data.clean()

    def get_xid(self):
        df = self.get_df_properties()
        xid = df["xid"].values.tolist()
        return xid

    def read_json_detailed(self):

        for object_id in self.get_xid():
            request = f"{base_url}xid/{object_id}?apikey" \
                      f"={api_key}"

            df = pd.read_json(request)
            req = requests.get(request)
            json = req.json()

            data = Prepare(df)
            address = data.address_to_list()

            for key in keys:
                if key in json:
                    del json[f"{key}"]

            json['address'] = address

            for dic_key in dic.keys():
                if dic_key not in json.keys():
                    json[dic_key] = 'null'

            get_schema(json)

        return dic

    def get_df_detailed(self):
        df = pd.DataFrame.from_dict(self.read_json_detailed())
        return df

    def create_csv(self):

        # merge properties + detailed on key xid
        df = self.get_df_properties().merge(self.get_df_detailed(), on='xid')

        df = df[schema]
        df = df.set_index(['xid'])

        df.to_csv('places_output.csv')
