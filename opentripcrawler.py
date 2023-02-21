
from pyspark.sql import SparkSession

class OpenTripCrawler:
    def __init__(self, config):
        self.config = config

        self.create_spark_sessions()
    
    def create_spark_sessions(self):
        self.spark_session = SparkSession.builder.appName("").getOrCreate()
    
    
    
