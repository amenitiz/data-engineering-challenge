
import requests
from pyspark.sql import SparkSession
from schemas import bbox_struct

class OpenTripCrawler:
    def __init__(self, config):
        self.config = config

        self.create_spark_sessions()

        self.get_accomodations_by_bbox()
    
    def create_spark_sessions(self):
        self.spark_session = SparkSession.builder.appName("").getOrCreate()

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
            df.show()
    
    
    
