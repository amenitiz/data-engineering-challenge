import os
import unittest

import src.models as models
import src.open_trip_map as pipeline
import tests.utils as utils


class TestGetPlacesInfo(unittest.TestCase):

    def setUp(self):
        self.bbox = [2.028471, 2.283903, 41.315758, 41.451768]
        self.kinds = 'accomodations'

    def test_number_of_elements(self):
        """
        Api call is returning the number of elements requested or less
        """

        limit = 3

        result = pipeline.get_places_info(self.bbox, self.kinds, limit)

        self.assertLessEqual(len(result), 3)


class TestPlacesToDf(utils.PySparkTests):

    def setUp(self):
        project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.data = utils.parse_json_file(f'{project_path}/tests/data/places_input.json')
        self.source = self.data['places']['raw']

    def test_data(self):
        """
        Verifies that the outputted dataframe has the correct fields and data types
        """

        expected = self.spark.createDataFrame(
            data=self.data['places']['processed'],
            schema=models.PlaceFlattened
        )

        result = pipeline.places_to_df(self.source, self.spark)

        self.assertTrue(utils.test_data(result, expected))

    def test_schema(self):
        """
        Test that data outputted by transform method has the correct schema
        """

        expected = self.spark.createDataFrame(
            data=self.data['places']['processed'],
            schema=models.PlaceFlattened
        )

        result = pipeline.places_to_df(self.source, self.spark)

        self.assertTrue(utils.test_schema(result, expected))


class TestPlacesDetailsToDf(utils.PySparkTests):

    def setUp(self):
        project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.data = utils.parse_json_file(f'{project_path}/tests/data/places_input.json')
        self.source = self.data['details']['raw']

    def test_data(self):
        """
        Test that data outputted by transform method has the expected data
        """

        expected = self.spark.createDataFrame(
            data=self.data['details']['processed'],
            schema=models.PlaceDetails
        )

        result = pipeline.places_details_to_df(self.source, self.spark)

        self.assertTrue(utils.test_data(result, expected))

    def test_schema(self):
        """
        Test that data outputted by transform method has the correct schema
        """

        expected = self.spark.createDataFrame(
            data=self.data['details']['processed'],
            schema=models.PlaceDetails
        )

        result = pipeline.places_details_to_df(self.source, self.spark)

        self.assertTrue(utils.test_schema(result, expected))


class TestTransform(utils.PySparkTests):

    def setUp(self):
        project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.data = utils.parse_json_file(f'{project_path}/tests/data/places_input.json')
        self.places_df = self.spark.createDataFrame(
            data=self.data['places']['processed'],
            schema=models.PlaceFlattened
        )
        self.details_df = self.spark.createDataFrame(
            data=self.data['details']['processed'],
            schema=models.PlaceDetails
        )

    def test_data(self):
        """
        Test that data outputted by transform method has the expected data
        """

        expected_result = self.spark.createDataFrame(
            data=self.data['results'],
            schema=models.PlacesWithDetailsResult
        )

        result = pipeline.transform(self.places_df, self.details_df)

        self.assertTrue(utils.test_data(result, expected_result))

    def test_schema(self):
        """
        Test that data outputted by transform method has the correct schema
        """

        expected_result = self.spark.createDataFrame(
            data=self.data['results'],
            schema=models.PlacesWithDetailsResult
        )

        result = pipeline.transform(self.places_df, self.details_df)

        self.assertTrue(utils.test_schema(result, expected_result))


class TestLoad(utils.PySparkTests):

    def setUp(self) -> None:
        self.project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.data = utils.parse_json_file(f'{self.project_path}/tests/data/places_input.json')

    def test_data(self):
        """
        Test that the written file has the expected data
        """

        source = self.spark.createDataFrame(
            data=self.data['results'],
            schema=models.PlacesWithDetailsResult)

        pipeline.load(source, f'{self.project_path}/output')
        result = self.spark.read.csv(
            path=f'{self.project_path}/output/',
            header=True,
            schema=models.PlacesWithDetailsResult
        )

        self.assertTrue(utils.test_data(result, source))

    def test_schema(self):
        """
        Test that the written file has the correct schema
        """

        source = self.spark.createDataFrame(
            data=self.data['results'],
            schema=models.PlacesWithDetailsResult)

        pipeline.load(source, f'{self.project_path}/output')
        result = self.spark.read.csv(
            path=f'{self.project_path}/output',
            header=True,
            schema=models.PlacesWithDetailsResult
        )

        self.assertTrue(utils.test_schema(result, source))


if __name__ == '__main__':
    unittest.main()
