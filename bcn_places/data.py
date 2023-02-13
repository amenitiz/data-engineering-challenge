import pandas as pd
from global_ import dic


def get_address(address_fields):
    address = ""
    for field in address_fields:
        address += field + " "
    return address


def get_schema(json):
    for key in list(json.keys()):
        if key == 'xid':
            dic['xid'].append(json.get('xid'))
        elif key == 'name':
            dic['name'].append(json.get('name'))
        elif key == 'kinds':
            dic['kinds'].append(json.get('kinds'))
        elif key == 'url':
            dic['url'].append(json.get('url'))
        elif key == 'stars':
            dic['stars'].append(json.get('stars'))
        elif key == 'wikipedia':
            dic['wikipedia'].append(json.get('wikipedia'))
        elif key == 'image':
            dic['image'].append(json.get('image'))
        elif key == 'address':
            dic['address'].append(json.get('address'))


class Prepare:

    def __init__(self, df):
        self.df = df

    def clean(self):
        # get lon + lat columns
        self.df[['lon', 'lat']] = self.df["point"].apply(lambda x: pd.Series(str(x).split(",")))
        df = self.df.drop('point', axis=1)
        df['lon'] = df['lon'].str[8:]
        df['lat'] = df['lat'].str[8:]
        df['lat'] = df['lat'].str[:-1]

        # filter
        df = df[df["kinds"].str.contains("skyscrapers")]

        # create kinds_amount column
        df['kinds'] = df['kinds'].str.split(',')
        df['kinds_amount'] = df['kinds'].apply(lambda x: len(x))

        df = df[['xid', 'kinds_amount', 'lon', 'lat']]

        return df

    def address_to_list(self):
        address = self.df["address"].values.tolist()
        address = [x for x in address if str(x) != 'nan']
        address = get_address(address)
        return address
