import os
import time
from platform import platform
from typing import Dict, List

import requests
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, size, split

from src.models import Place, PlaceDetails


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

    try:
        response = requests.get(
            params=query,
            url=url
        )
        response.raise_for_status()

        return response.json()
    except requests.exceptions.HTTPError():
        return []


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

    try:
        response = requests.get(
            params=query,
            url=url
        )
        response.raise_for_status()
        print(f'[INFO] - {time.time()}')
        return response.json()
    except requests.exceptions.HTTPError():
        return {}


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
    requests_threshold = 5
    requests_performed = 0

    for xid in xids:
        details = get_place_details(xid)
        places_details.append(details)
        requests_performed += 1

        if requests_performed == requests_threshold:
            time.sleep(1)
            requests_performed = 0

    return places_details


def extract(number_of_places: int) -> List[Dict[str, str]]:
    """
    Returns a list of places from opentripmap
    :param number_of_places: number of objects to return from openapi (max 5000)
    :type number_of_places: int
    :return: list of places objects
    :rtype: List[Dict[str, str]]
    """

    if number_of_places > 5000:
        raise ValueError('Daily number of requests exceeded, max number allowed is 5000')

    places = get_places_info(
        bbox=[2.028471, 2.283903, 41.315758, 41.451768],
        kinds='accomodations',
        limit=number_of_places
    )

    return places


def transform(places: List[Dict[str, str]], spark: SparkSession) -> DataFrame:
    """
    Returns a dataframe with places filtering out skyscrapers kind and adding
    details fetched for every single id passed in places

    :param places: list of places object
    :type places: List[Dict[str, str]]
    :param spark: spark session
    :type spark: SparkSession
    :return: a dataframe with all the transformations described above
    :rtype: DataFrame
    """

    accommodations = spark.createDataFrame(data=places, schema=Place)
    accommodations_flattened = (
        accommodations.withColumn('lon', col('point').getField('lon'))
        .withColumn('lat', col('point').getField('lat'))
        .drop('point')
    )
    no_skyscraper_accommodations = (
        accommodations_flattened.filter(~(col('kinds').contains('skyscrapers')))
    )
    add_number_of_kinds = (
        no_skyscraper_accommodations.withColumn('kinds_amount', size(split(col('kinds'), ',')))
    )

    places_ids = add_number_of_kinds.select('xid').collect()

    places_details = get_places_details([row.xid for row in places_ids])

    places_details_df = spark.createDataFrame(places_details, PlaceDetails)

    places_details_flatten = (
        places_details_df.withColumn('country_code', col('address').getField('country_code'))
        .withColumn('country', col('address').getField('country'))
        .withColumn('city', col('address').getField('city'))
        .withColumn('postcode', col('address').getField('postcode').cast('integer'))
        .withColumn('county', col('address').getField('county'))
        .withColumn('suburb', col('address').getField('suburb'))
        .withColumn('house_number', col('address').getField('house_number').cast('integer'))
        .withColumn('pedestrian', col('address').getField('pedestrian'))
        .withColumn('state', col('address').getField('state'))
        .withColumn('city_district', col('address').getField('city_district'))
        .drop('address')
    )

    return add_number_of_kinds.join(places_details_flatten, on='xid')


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

        data = extract(50)

        processed_data = transform(data, s)

        load(processed_data, f'{os.getcwd()}/output/places_output.csv')
