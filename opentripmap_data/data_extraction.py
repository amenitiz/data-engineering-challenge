# Extract 2500 entries from Open Trip Map
import os

import pandas as pd
import requests

OPENTRIPMAP_URL = "https://api.opentripmap.com"
VERSION = "0.1"
LANGAUGE = "en"
LOCATION_ENDPOINT = "places/bbox"
DETAILS_ENDPOINT = "places/xid"
FORMAT = "json"
KINDS = "accomodations"
MIN_LONGITUDE = "2.028471"
MAX_LONGITUDE = "2.283903"
MIN_LATITUDE = "41.315758"
MAX_LATITUDE = "41.451768"
LIMIT = 2500
API_KEY = os.getenv("OPENTRIPMAP_APIKEY")

request_parameters = {
    "kinds": KINDS,
    "lon_min": MIN_LONGITUDE,
    "lon_max": MAX_LONGITUDE,
    "lat_min": MIN_LATITUDE,
    "lat_max": MAX_LATITUDE,
    "format": FORMAT,
    "limit": LIMIT,  # TODO - this parameter is not having any impact on the payload.
    "apikey": API_KEY,
}


def extract_data(url, version, language, endpoint, params=None):
    r = requests.get(f"{url}/{version}/{language}/{endpoint}", params=params)

    # Validate the data extracted from Open Trip Map
    print(f"{r.status_code}") if r.status_code == 200 else print(
        f"Something went wrong, "
        f"API returned {r.status_code} response. with details {r.json()} "
    )

    try:
        print(f"{len(r.json())} rows entries extracted correctly.") if len(
            r.json()
        ) == params["limit"] else print(
            f"Something went wrong, expected {params['limit']} entries but got {len(r.json())}."
        )
    except KeyError:
        pass

    return r.json()


def parse_opentripmap_location_entry(entry):
    # Format the entry to be compatible with the Pandas dataframe used for this challenge
    try:
        osm = entry["osm"]
    except KeyError:
        osm = None

    try:
        wikidata = entry["wikidata"]
    except KeyError:
        wikidata = None

    return [
        {
            "xid": entry["xid"],
            "name": entry["name"],
            "rate": entry["rate"],
            "osm": osm,
            "wikidata": wikidata,
            "kinds": entry["kinds"],
            "longitude": entry["point"]["lon"],
            "latitude": entry["point"]["lat"],
        }
    ]


def parse_location_detail_address(full_address):
    try:
        number = full_address["house_number"]
    except KeyError:
        number = full_address["house"]
    return (
        f'{full_address["road"]}, {number}. '
        f'{full_address["postcode"]} {full_address["suburb"]}({full_address["county"]}). {full_address["country"]}'
    )


def parse_opentripmap_location_detail_entry(location_detail):
    try:
        stars = location_detail["stars"]
    except KeyError:
        stars = None

    return [
        {
            "xid": location_detail["xid"],
            "stars": stars,
            "address": parse_location_detail_address(location_detail["address"]),
            "url": location_detail["url"],
            "image": location_detail["image"],
            "wikipedia": location_detail["wikipedia"],
        }
    ]


def store_locations_in_dataframe(locations):
    # Create Dataframe
    df = pd.DataFrame(
        columns=[
            "xid",
            "name",
            "rate",
            "osm",
            "wikidata",
            "kinds",
            "longitude",
            "latitude",
        ]
    )

    # Append all entries
    for location in locations:
        df = pd.concat(
            [df, pd.DataFrame(parse_opentripmap_location_entry(location))],
            axis=0,
            ignore_index=True,
        )
    return df


def extract_locations_data_from_opentripmap_to_df(
    url=OPENTRIPMAP_URL,
    version=VERSION,
    language=LANGAUGE,
    endpoint=LOCATION_ENDPOINT,
    params=request_parameters,
):
    locations = extract_data(
        url=url, version=version, language=language, endpoint=endpoint, params=params
    )

    # Store json data in Pandas/Pyspark dataframe
    return store_locations_in_dataframe(locations=locations)


def get_xids_from_locations_dataframe(locations_df):
    # Returns a python list with all the Xids of every location in the dataframe
    return locations_df["xid"].values.tolist()


def create_location_details_df(
    locations_df,
    url=OPENTRIPMAP_URL,
    version=VERSION,
    language=LANGAUGE,
    endpoint=DETAILS_ENDPOINT,
):
    # Get list of xids
    xid_list = get_xids_from_locations_dataframe(locations_df)

    # Create Details Dataframe
    df = pd.DataFrame(columns=["xid", "stars", "address", "url", "image", "wikipedia"])

    # Extract detail for each location and store it in dataframe
    for xid in xid_list:
        location_detail = extract_data(
            url=url,
            version=version,
            language=language,
            endpoint=f"{endpoint}/{xid}",
            params={"apikey": API_KEY},
        )
        df = pd.concat(
            [
                df,
                pd.DataFrame(parse_opentripmap_location_detail_entry(location_detail)),
            ],
            axis=0,
            ignore_index=True,
        )
    return df
