
import requests
from pyspark.sql import SparkSession
from schemas import bbox_struct, extra_fields_struct
from pyspark.sql.functions import col, size, split
from pyspark.sql.types import *
from utils import load_json, check_path, save_csv


class OpenTripCrawler:
    def __init__(self, config):
        self.config = config

        self.create_spark_sessions()

    def run(self):
        accomodations_df = self.flat_bbox_df(self.get_accomodations_by_bbox())

        accomodations_df = self.filter_by_col_keywords(accomodations_df, "kinds", "skyscrapers")

        accomodations_df = self.count_column_per_place(accomodations_df, "kinds", "name")

        extra_df = self.flat_extra_fields_df(self.get_extra_accomodation_fields(accomodations_df))

        self.final_df = self.merge_dfs(accomodations_df, extra_df, "xid")
    
    def create_spark_sessions(self):
        self.spark_session = SparkSession.builder.appName("OpenTrip Map API Crawler").getOrCreate()

    def get_accomodations_by_bbox(self):
        
        query = {
                'apikey': self.config["apikey"],
                'format': self.config["format"],
                'kinds': self.config["kinds"],
                'limit': self.config["limit"],
                'lon_min': self.config["bbox"][0],
                'lon_max': self.config["bbox"][1],
                'lat_min': self.config["bbox"][2],
                'lat_max': self.config["bbox"][3]
            }

        response = None
        objects = []

        while len(objects) < 2500:
            response = requests.get(
                params=query,
                url=self.config["bbox_url"]
            )
            objects += response.json()

        df = self.spark_session.createDataFrame(data=objects, \
                                                schema = bbox_struct)

        if self.config["debug"]:
            print("DataFrame with 2500 accomodation's objects")
            df.show()


        return df
        
    def flat_bbox_df(self, df):

        df = df.select(col("xid"), col("kinds"), col("name"), 
                                col("point.lon").alias("lon"),
                                col("point.lat").alias("lat"),
                                col("osm"),
                                col("rate"),
                                col("wikidata"))

        if self.config["debug"]:
            print("Flattened DataFrame")
            df.show()
                
        return df

    def filter_by_col_keywords(self, df, col, keyword):
        
        df = df.filter(df[col].contains(keyword))
        
        if self.config["debug"]:
            print(f"Filtered column {col} by keyword {keyword}")
            df.show()

        return df
    
    def count_column_per_place(self, df, col, place_col):

        df = df.withColumn('kinds_amount', size(split(df[col], ',')))

        if self.config["debug"]:

            df.filter(df.name == 'Hotel Ginebra').show()

            df.show()

        return df

    def get_extra_accomodation_fields(self, df):

        xids_list = df.select('xid').rdd.flatMap(lambda x: x).collect()

        query = {"apikey": self.config["apikey"]}

        list_urls = [f'{self.config["xid_url"]}{xid}' for xid in xids_list]
        
        responses = []
        for url in list_urls:
            response = requests.get(url, params=query).json()

            responses.append(Row(response.get('xid'), \
                            response.get('stars'),
                            response.get('address'),
                            response.get('url'),
                            response.get('image'),
                            response.get('wikipedia')
                    ))
        
        df = self.spark_session.createDataFrame(responses, \
                                            schema = extra_fields_struct)

        return df
    
    def flat_extra_fields_df(self, df):
        
        df = df.select(col("xid"), col("stars"), 
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
                          col("wikipedia"))

        #if self.config["debug"]:
        df.show()

        save_csv(df, "extraflatdf.csv")

        return df
    
    def merge_dfs(self, df1, df2, column):
        return df1.join(df2, df1[column] ==  df2[column], "inner").drop(df1.xid)
    
    def get_accomodations_df(self):
        return self.final_df

    
