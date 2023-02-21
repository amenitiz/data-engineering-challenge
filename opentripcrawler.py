
import requests
from pyspark.sql import SparkSession
from schemas import bbox_struct
from pyspark.sql.functions import col, size, split

class OpenTripCrawler:
    def __init__(self, config):
        self.config = config

        self.create_spark_sessions()

        accomodations_df = self.get_accomodations_by_bbox()

        accomodations_df = self.filter_by_col_keywords(accomodations_df, "kinds", "skyscrapers")

        self.accomodations_df = self.count_column_per_place(accomodations_df, "kinds", "name")
    
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

        response = requests.get(
            params=query,
            url=self.config["bbox_url"]
        )

        df = self.spark_session.createDataFrame(data=response.json(), \
                                                schema = bbox_struct)

        if self.config["debug"]:
            print("DataFrame with 2500 accomodation's objects")
            df.show()

        df = self.flat_bbox_df(df)

        return df
    
    def flat_bbox_df(self, df):

        df = df.select(col("kinds"), col("name"), 
                                col("osm"),
                                col("point.lon").alias("lon"),
                                col("point.lat").alias("lat"),
                                col("rate"),
                                col("wikidata"),
                                col("xid"))

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

        #df = df.groupBy(place_col).sum("kind_amounts")

        if self.config["debug"]:

            df.filter(df.name == 'Hotel Ginebra').show()

            df.show()

        return df

    def get_accomodations_df(self):
        return self.accomodations_df

    
