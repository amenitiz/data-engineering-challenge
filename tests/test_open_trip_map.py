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

        self.assertEqual(len(result), 3)


class TestPlacesToDf(utils.PySparkTests):

    def setUp(self):
        project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.test_data = utils.parse_json_file(f'{project_path}/tests/data/places_input.json')

    def test_output(self):
        """
        Verifies that the output has the correct data and schema
        """

        source = self.test_data['places']['raw']
        expected = self.spark.createDataFrame(
            data=self.test_data['places']['processed'],
            schema=models.PlaceFlattened
        )

        result = pipeline.places_to_df(source, self.spark)

        self.assertTrue(utils.test_data(result, expected))
        self.assertTrue(utils.test_schema(result, expected))


class TestPlacesDetailsToDf(utils.PySparkTests):

    def setUp(self):
        project_path = os.environ.get('PROJECT_ROOT_PATH')
        self.test_data = utils.parse_json_file(f'{project_path}/tests/data/places_input.json')

    def test_output(self):
        """
        Verifies that the outputted dataframe has the correct fields and data types
        """

        source = self.test_data['details']['raw']
        expected = self.spark.createDataFrame(
            data=self.test_data['details']['processed'],
            schema=models.PlaceDetails
        )

        result = pipeline.places_details_to_df(source, self.spark)

        self.assertTrue(utils.test_data(result, expected))
        self.assertTrue(utils.test_schema(result, expected))


if __name__ == '__main__':
    unittest.main()
