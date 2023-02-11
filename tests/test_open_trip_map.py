import unittest

from src.open_trip_map import get_places_info


class TestGetPlacesInfo(unittest.TestCase):

    def setUp(self):
        # self.sparkSession = new_spark_session('test_get_places_info')
        self.bbox = [2.028471, 2.283903, 41.315758, 41.451768]
        self.kinds = 'accomodations'

    def test_number_of_elements(self):
        """
        Api call is returning the number of elements requested or less
        """

        limit = 300

        result = get_places_info(self.bbox, self.kinds, limit)

        self.assertEqual(len(result), 300)

    def tearDown(self):
        pass
        # self.sparkSession.stop()


if __name__ == '__main__':
    unittest.main()
