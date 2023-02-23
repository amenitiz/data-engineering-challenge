import requests
from pyspark.sql import SparkSession
from schemas import bbox_struct, extra_fields_struct
from pyspark.sql.functions import col, size, split
from pyspark.sql.types import Row
import time


class OpenTripCrawler:
    """
    Class that applies ETL pipeline on objects extracted from OpenTripMap API.
    A csv file is generated with the final data.

    Attributes
    ----------
    config : json
        The data extracted from the API depends on the configurations set on
        the "config.json" file:

        * bbox - sets the geographic area where we want
        extract data (longitude min, longitude max,
        latitude min, latitude max)

        * limit - maximum number of objects to be extracted

        * kinds - Object category.

        * format - The output format (csv, json, ...)

        * apikey - API key to get information from the API

        * bbox_url - url to get objects from API

        * xid_url - url to get objects by xid

    """

    def __init__(self, config):
        """
        Constructs the needed attributes for the OpenTripMap object.

        Parameters
        ----------
            config : json
        """

        self.config = config

        self.create_spark_sessions()

    def run(self):
        """
        Executes all the ETL steps to get the final dataframe.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        accomodations_df = self.flat_bbox_df(self.get_accomodations_by_bbox())

        accomodations_df = self.filter_by_col_keywords(
            accomodations_df, "kinds", "skyscrapers"
        )

        accomodations_df = self.count_column_per_place(accomodations_df, "kinds")

        extra_df = self.flat_extra_fields_df(
            self.get_extra_accomodation_fields(accomodations_df)
        )

        self.final_df = self.merge_dfs(accomodations_df, extra_df, "xid")

    def create_spark_sessions(self):
        """
        Sets Spark Session object.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.spark_session = SparkSession.builder.appName(
            "OpenTrip Map API Crawler"
        ).getOrCreate()

    def get_accomodations_by_bbox(self):
        """
        FRQ-01 - Extracts 2500 objects from OpenTripMap in english.

        Parameters
        ----------
        None

        Returns
        -------
        df: DataFrame
            PySpark Dataframe with 2500 rows for specified kinds
            and geographic area (bbox).

        """

        query = {
            "apikey": self.config["apikey"],
            "format": self.config["format"],
            "kinds": self.config["kinds"],
            "limit": self.config["limit"],
            "lon_min": self.config["bbox"][0],
            "lon_max": self.config["bbox"][1],
            "lat_min": self.config["bbox"][2],
            "lat_max": self.config["bbox"][3],
        }

        response = None
        objects = []

        while len(objects) < 2500:
            try:
                response = requests.get(params=query, url=self.config["bbox_url"])
                objects += response.json()
            except:
                time.sleep(10)

        df = self.spark_session.createDataFrame(
            data=objects[0:2500], schema=bbox_struct
        )

        if self.config["debug"]:
            print("DataFrame with 2500 accomodation's objects")
            df.show()

        return df

    def flat_bbox_df(self, df):
        """
        FRQ-02 - Get data in flat DataFrame

        Parameters
        ----------
        df: DataFrame
            PySpark Dataframe with 2500 rows for specified kinds
            and geographic area (bbox).

        Returns
        -------
        df: DataFrame
            Flat PySpark Dataframe.

        """

        df = df.select(
            col("xid"),
            col("kinds"),
            col("name"),
            col("point.lon").alias("lon"),
            col("point.lat").alias("lat"),
            col("osm"),
            col("rate"),
            col("wikidata"),
        )

        if self.config["debug"]:
            print("Flattened DataFrame")
            df.show()

        return df

    def filter_by_col_keywords(self, df, col, keyword):
        """
        FRQ-03 - Filter column by keyword.

        Parameters
        ----------
        df: DataFrame
            PySpark Dataframe.
        col: str
            Column name to be filtered.
        keyword: str
            Keyword to filter column with.

        Returns
        -------
        df: DataFrame
            PySpark Dataframe.

        """

        df = df.filter(df[col].contains(keyword))

        if self.config["debug"]:
            print(f"Filtered column {col} by keyword {keyword}")
            df.show()

        return df

    def count_column_per_place(self, df, col):
        """
        FRQ-04 - Count number of column values per place.
        This function assumes that the column values are
        strings with values separated by columns.

        Parameters
        ----------
        df: DataFrame
            PySpark Dataframe.
        col: str
            Column name.

        Returns
        -------
        df: DataFrame
            PySpark Dataframe.

        """

        df = df.withColumn("kinds_amount", size(split(df[col], ",")))

        if self.config["debug"]:

            df.filter(df.name == "Hotel Ginebra").show()

            df.show()

        return df

    def get_extra_accomodation_fields(self, df):
        """
        FRQ-05 - Get from the OpenTripMap API extra fields
        for each xid extracted before.

        Parameters
        ----------
        df: DataFrame
            df: DataFrame
            PySpark Dataframe with 2500 rows for specified kinds
            and geographic area (bbox). Must contain column 'xid'.

        Returns
        -------
        df: DataFrame
            Flat PySpark Dataframe.

        """

        xids_list = df.select("xid").rdd.flatMap(lambda x: x).collect()

        query = {"apikey": self.config["apikey"]}

        list_urls = [f'{self.config["xid_url"]}{xid}' for xid in xids_list]

        responses = []
        for url in list_urls:

            response = requests.get(url, params=query).json()

            responses.append(
                Row(
                    response.get("xid"),
                    response.get("stars"),
                    response.get("address"),
                    response.get("url"),
                    response.get("image"),
                    response.get("wikipedia"),
                )
            )

        df = self.spark_session.createDataFrame(responses, schema=extra_fields_struct)

        return df

    def flat_extra_fields_df(self, df):
        """
        FRQ-05 - Get data in flat DataFrame

        Parameters
        ----------
        df: DataFrame
            PySpark Dataframe with the extra fields.

        Returns
        -------
        df: DataFrame
            Flat PySpark Dataframe.

        """

        df = df.select(
            col("xid"),
            col("stars"),
            col("address.city").alias("city"),
            col("address.house").alias("house"),
            col("address.state").alias("state"),
            col("address.county").alias("county"),
            col("address.suburb").alias("suburb"),
            col("address.country").alias("country"),
            col("address.postcode").alias("postcode"),
            col("address.pedestrian").alias("pedestrian"),
            col("address.country_code").alias("country_code"),
            col("address.house_number").alias("house_number"),
            col("address.city_district").alias("city_district"),
            col("address.neighbourhood").alias("neighbourhood"),
            col("url"),
            col("image"),
            col("wikipedia"),
        )

        if self.config["debug"]:
            df.show()

        return df

    def merge_dfs(self, df1, df2, column):
        """
        FRQ-05 - Joins the two dataframes.

        Parameters
        ----------
        df1: DataFrame
            PySpark Dataframe with the bbox fields.
        df2: DataFrame
            PySpark Dataframe with the extra fields.
        column: str
            Primary key name.

        Returns
        -------
        df: DataFrame
            Merged PySpark Dataframe.

        """

        return (
            df1.join(df2, df1[column] == df2[column], "left")
            .drop(df2[column])
            .dropDuplicates()
        )

    def get_accomodations_df(self):
        """
        Returns final dataframe.

        Parameters
        ----------
        None

        Returns
        -------
        df: DataFrame
            Final Dataframe.

        """
        return self.final_df
