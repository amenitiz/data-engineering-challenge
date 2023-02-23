import unittest
from pyspark.sql import SparkSession
from schemas import bbox_struct, extra_fields_struct, flat_bbox_struct, flat_extra_field_struct, kinds_bbox_struct, final_struct
from opentripcrawler import OpenTripCrawler
from utils import load_json, check_path
from config_unitests import *

class UnitTestsSpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Constructs the needed attributes for the UnitTests Class object.

        Parameters
        ----------
            None
        """

        cls.open_object = OpenTripCrawler(load_json(check_path("config.json")))

        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())
    
    def run_asserts(self, transformed_df, expected_df):
        """
        Runs asserts for data types and counts.
        
        Parameters
        ----------
        transformed_df: DataFrame
            DataFrame generated to be tested.

        expected_df: DataFrame
            Expected DataFrame.

        Returns
        -------
        None

        """

        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        
        res = set(fields1) == set(fields2)

        self.assertTrue(res)

        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))
    
    def create_dataframes(self, input_data, input_schema, expected_data, expected_schema, input_data2 = None, expected_schema2 = None):
        """
        Creates PySpark DataFrames.
        
        Parameters
        ----------
        input_data: DataFrame
            Input data before transformation.

        input_schema: StructType
            Input data schema.

        expected_data: DataFrame
            Expected data after transformation.

        expected_schema: StructType
            Expected data schema.
        
        input_data2: DataFrame (optional)
            Input data before transformation.

        input_schema2: StructType (optional)
            Input data schema.

        Returns
        -------
        input_df: DataFrame
            Input DataFrame before transformation.
        expected_df: DataFrame
            Expected DataFrame before transformation.
        input_df2: DataFrame
            Input DataFrame before transformation.

        """

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        if input_data2 and expected_schema2:
            input_df2 = self.spark.createDataFrame(data=input_data2, schema=expected_schema2)

            return input_df, expected_df, input_df2
    
        else:
            return input_df, expected_df

    def test_bbx(self):
        """
        Test flat_bbox_df function.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        
        input_df, expected_df = self.create_dataframes(input_data_bbx, bbox_struct, expected_data_bbx, flat_bbox_struct)

        transformed_df = self.open_object.flat_bbox_df(input_df)

        self.run_asserts(transformed_df, expected_df)

    def test_filtering(self):
        """
        Test filter_by_col_keywords function.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """

        input_df, expected_df = self.create_dataframes(input_data_filtering, flat_bbox_struct, expected_data_filtering, flat_bbox_struct)

        transformed_df = self.open_object.filter_by_col_keywords(input_df, "kinds", "skyscrapers")

        self.run_asserts(transformed_df, expected_df)

    def test_count(self):
        """
        Test count_column_per_place function.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """

        input_df, expected_df = self.create_dataframes(input_data_count, flat_bbox_struct, expected_data_count, kinds_bbox_struct)

        transformed_df = self.open_object.count_column_per_place(input_df, "kinds")

        self.run_asserts(transformed_df, expected_df)

    def test_extra(self):
        """
        Test flat_extra_fields_df function.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """

        input_df, expected_df = self.create_dataframes(input_data_extra, extra_fields_struct, expected_data_extra, flat_extra_field_struct)

        transformed_df = self.open_object.flat_extra_fields_df(input_df)

        self.run_asserts(transformed_df, expected_df)

    def test_final_dataframe(self):  
        """
        Test merge_dfs function.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """

        input_df1, expected_df, input_df2 = self.create_dataframes(input_data_final1, kinds_bbox_struct, expected_data_final, final_struct, input_data_final2, flat_extra_field_struct)

        transformed_df = self.open_object.merge_dfs(input_df1, input_df2, "xid")

        self.run_asserts(transformed_df, expected_df)

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark Session.
        
        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()