import requests
import pandas as pd
from global_ import url, api_key, keys, dic, schema, url_part_detailed
from data import Prepare, get_schema


class Model:
    def __init__(self):
        self.url = url
        self.key = api_key

    def get_df_properties(self):
        df = pd.read_json(f"{self.url}&apikey={self.key}")

        data = Prepare(df)

        return data.clean()

    def get_xid(self):
        df = self.get_df_properties()
        xid = df["xid"].values.tolist()
        return xid

    def read_json_detailed(self):

        for object_id in self.get_xid():
            request = f"{url_part_detailed}{object_id}?apikey" \
                      f"={api_key}"

            df = pd.read_json(request)

            data = Prepare(df)
            address = data.address_to_list()

            req = requests.get(request)
            json = req.json()

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
