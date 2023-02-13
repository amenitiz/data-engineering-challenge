import logging
import os
import time
from platform import platform
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, size, split

from src.models import PlaceRaw, PlaceDetailsRaw


def perform_rest_call(method: str, params: Dict[str, str], url: str, timeout: int = 3) -> dict:
    """
    Performs a rest call.
    Has a retry policy for 409 errors.
    Exists the program if an exception occurs.

    :param method: HTTP method
    :type method: str
    :param params: key value pairs passed in the query string
    :type params: str
    :param url:
    :type url: str
    :param timeout: number of seconds to wait for response before failing
    :type timeout: int
    :return: response in json format
    :rtype: dict
    """

    try:
        # 409 errors happen more often than expected even performing 5 request per second.
        # we're implementing a retry on them, so we can delete the wait logic from the
        # extract details function
        session = requests.Session()
        retries = Retry(
            backoff_factor=0.4,
            status_forcelist=[409, 500, 502, 503, 504],
            total=3
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))

        response = session.request(
            method=method,
            params=params,
            timeout=timeout,
            url=url
        )

        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    return response.json()


def new_spark_session(app_name: str) -> SparkSession:
    """Creates a new spark session with application name

    :param app_name: application name
    :type app_name: str
    :return: new spark session
    :rtype: SparkSession
    """

    session = SparkSession.builder.master('local').appName(app_name)

    # Configuration needed to be able to save output in windows fs
    # https://stackoverflow.com/questions/40308340/scala-spark-dataframe-write-on-windows
    if 'Windows' in platform():
        session.config("spark.sql.warehouse.dir", "file:///C:/IJava/")

    session = session.getOrCreate()

    return session


def get_places_info(bbox: List[float], kinds: str, limit: int) -> List[Dict[str, str]]:
    """
    Performs and get call to opentripmap api and returns a dictionary with

    :param bbox: lon_min, lon_max, lat_min and lat_max coordinates te restrict places to fetch
    :type bbox: List[float]
    :param kinds: comma separated list of kinds to fetch
    :type kinds: str
    :param limit: max number of objects to retrieve
    :type limit: int
    :return: list of objects as described in https://opentripmap.io/docs#/Objects%20list/getListOfPlacesByLocation
    :rtype: List[Dict[str, str]]
    """

    url = 'https://api.opentripmap.com/0.1/en/places/bbox'
    query = {
        'apikey': os.environ.get('OPENTRIP_API_TOKEN'),
        'format': 'json',
        'kinds': kinds,
        'limit': limit,
        'lon_min': bbox[0],
        'lon_max': bbox[1],
        'lat_min': bbox[2],
        'lat_max': bbox[3]
    }

    return perform_rest_call('get', query, url)


def get_place_details(xid: str) -> Dict[str, str]:
    """
    Perform a rest call to opentripmap and retrieve full details for a place

    :param xid: id of the place to search on opentripmap
    :type xid: str
    :return: object as described in https://opentripmap.io/docs#/Object%20properties/getPlaceByXid
    :rtype: Dict[str, str]
    """

    url = f'https://api.opentripmap.com/0.1/en/places/xid/{xid}'
    query = {
        'apikey': '5ae2e3f221c38a28845f05b613ca228a2d016638e03f5f81315b3415'
    }

    return perform_rest_call('get', query, url)


def get_places_details(xids: List[str]) -> List[Dict[str, str]]:
    """
    Iterates over xids and fetches and retrieves all their details from opentripmap.
    To avoid incurring into 409 errors the function waits 1 second every 5 requests.

    :param xids: List of ids of places to search
    :type xids: List[str]
    :return: List of places
    :rtype: List[Dict[str, str]]
    """

    places_details = []

    for xid in xids:
        details = get_place_details(xid)
        places_details.append(details)

    return places_details


def places_to_df(places: List[Dict[str, str]], spark: SparkSession) -> DataFrame:
    """
    Transforms list of places objects into a df.
    Extracts lon and lat fields from point field

    :param places: list of places retrieved from opentripmap apis
    :type places: List[Dict[str, str]
    :param spark: a spark session on which to create the dataframe
    :type spark: SparkSession
    :return: places objects converted into a spark df
    :rtype: DataFrame
    """

    places = spark.createDataFrame(data=places, schema=PlaceRaw)
    extract_lon_and_lat = (
        places.withColumn('lon', col('point').getField('lon'))
        .withColumn('lat', col('point').getField('lat'))
        .drop('point')
    )

    return extract_lon_and_lat


def places_details_to_df(places_details: List[Dict[str, str]], spark: SparkSession) -> DataFrame:
    """
    Takes a list of places details as inputs, transforms it into a dataframe with the PlacesDetails schema.
    Extracts all fields from the address column into their own

    :param places_details: list of places details
    :type: places_details: List[Dict[str, str]
    :param spark: spark session
    :type spark: SparkSession
    :return: places details converted into a df
    :rtype: DataFrame
    """

    df = spark.createDataFrame(places_details, PlaceDetailsRaw)

    flatten_addresses = (
        df.withColumn('country_code', col('address').getField('country_code'))
        .withColumn('country', col('address').getField('country'))
        .withColumn('city', col('address').getField('city'))
        .withColumn('postcode', col('address').getField('postcode'))
        .withColumn('county', col('address').getField('county'))
        .withColumn('suburb', col('address').getField('suburb'))
        .withColumn('house_number', col('address').getField('house_number').cast('integer'))
        .withColumn('pedestrian', col('address').getField('pedestrian'))
        .withColumn('state', col('address').getField('state'))
        .withColumn('city_district', col('address').getField('city_district'))
        .drop('address')
    )

    return flatten_addresses


def extract(number_of_places: int, spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    Fetches objects places and its details from api, converts each of then into a dataframe
    and returns them inside a tuple

    :param number_of_places: number of objects to return from openapi (max 5000)
    :type number_of_places: int
    :param spark: spark session
    :type spark: SparkSession
    :return: places and places_details dataframes
    :rtype: Tuple[DataFrame, DataFrame]
    """

    if number_of_places > 5000:
        raise ValueError('Daily number of requests exceeded, max number allowed is 5000')

    places = get_places_info(
        bbox=[2.028471, 2.283903, 41.315758, 41.451768],
        kinds='accomodations',
        limit=number_of_places
    )

    places_df = places_to_df(places, spark)

    places_ids = [place['xid'] for place in places]

    places_details = get_places_details(places_ids)

    details_df = places_details_to_df(places_details, spark)

    return places_df, details_df


def transform(places: DataFrame, places_details: DataFrame) -> DataFrame:
    """
    Returns a dataframe with places filtering out skyscrapers kind and adding
    details fetched for every single place id

    :param places: places df
    :type places: DataFrame
    :param places_details: places details df
    :type places_details: DataFrame
    :return: a dataframe with all the transformations described above
    :rtype: DataFrame
    """

    no_skyscraper_accommodations = (
        places.filter(~(col('kinds').contains('skyscrapers')))
    )
    add_number_of_kinds = (
        no_skyscraper_accommodations.withColumn('kinds_amount', size(split(col('kinds'), ',')))
    )

    return add_number_of_kinds.join(places_details, on='xid')


def load(detailed_places: DataFrame, path: str) -> None:
    """
    Saves detailed places dataframe on the path indicated

    :param detailed_places: dataframe to save
    :type detailed_places: DataFrame
    :param path: location on the filesystem to save the dataframe
    :type path: str
    """

    detailed_places.write.mode('overwrite').option('header', True).csv(path)


if __name__ == '__main__':

    with new_spark_session('Open Trip Map') as s:

        data = extract(50, s)

        processed_data = transform(data[0], data[1])

        load(processed_data, f'{os.environ.get("PROJECT_ROOT_PATH")}/output/places_output.csv')
