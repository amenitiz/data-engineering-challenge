import unittest
from pyspark.sql import SparkSession
from schemas import bbox_struct, extra_fields_struct, flat_bbox_struct, flat_extra_field_struct, kinds_bbox_struct
from opentripcrawler import OpenTripCrawler
from utils import load_json, check_path, save_csv

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.open_object = OpenTripCrawler(load_json(check_path("config.json")))

        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())
    
    def run_asserts(self, transformed_df, expected_df):
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    def test_bbx(self):
        
        input_schema = bbox_struct
                                        
        input_data = [
                ("R10795366", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "Hotel Espanya", {"lon": "2.173105478", "lat": "41.37990189"}, "relation/10795366", "7", "Q11682616"),
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona", {"lon": "2.196222067", "lat": "41.38689041"}, "way/35816740", "7", "Q1425790"),
                ("N2967989788", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel lloret ramblas", {"lon": "2.170171976", "lat": "41.38481522"}, "node/2967989788", "7", "Q47170813"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", {"lon": "2.218544006", "lat": "41.41080856"}, "way/206962976", "7", "Q5911239"),
                ("W666948775","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel el castell", {"lon": "2.044023991", "lat": "41.34471512"}, "way/666948775", "7", "Q65209044"),
                ("N1681460715","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "park hotel", {"lon": "2.184364557", "lat": "41.38423538"}, "node/1681460715", "7", "Q47170780")]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = flat_bbox_struct
         
        expected_data = [
                ("R10795366", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "Hotel Espanya", "2.173105478", "41.37990189", "relation/10795366", "7", "Q11682616"),
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona",  "2.196222067", "41.38689041", "way/35816740", "7", "Q1425790"),
                ("N2967989788", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel lloret ramblas", "2.170171976", "41.38481522", "node/2967989788", "7", "Q47170813"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", "2.218544006", "41.41080856", "way/206962976", "7", "Q5911239"),
                ("W666948775","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel el castell", "2.044023991", "41.34471512", "way/666948775", "7", "Q65209044"),
                ("N1681460715","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "park hotel", "2.184364557", "41.38423538", "node/1681460715", "7", "Q47170780")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        #3. Apply our transformation to the input data frame
        transformed_df = self.open_object.flat_bbox_df(input_df)

        self.run_asserts(transformed_df, expected_df)

    def test_filtering(self):

        input_schema = flat_bbox_struct
                                        
        input_data = [
                ("R10795366", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "Hotel Espanya", "2.173105478", "41.37990189", "relation/10795366", "7", "Q11682616"),
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona",  "2.196222067", "41.38689041", "way/35816740", "7", "Q1425790"),
                ("N2967989788", "architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel lloret ramblas", "2.170171976", "41.38481522", "node/2967989788", "7", "Q47170813"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", "2.218544006", "41.41080856", "way/206962976", "7", "Q5911239"),
                ("W666948775","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel el castell", "2.044023991", "41.34471512", "way/666948775", "7", "Q65209044"),
                ("N1681460715","architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "park hotel", "2.184364557", "41.38423538", "node/1681460715", "7", "Q47170780")]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        
        expected_data = [
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona",  "2.196222067", "41.38689041", "way/35816740", "7", "Q1425790"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", "2.218544006", "41.41080856", "way/206962976", "7", "Q5911239")]
        
        expected_schema = flat_bbox_struct

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = self.open_object.filter_by_col_keywords(input_df, "kinds", "skyscrapers")

        self.run_asserts(transformed_df, expected_df)

    def test_count(self):

        input_schema = flat_bbox_struct

        input_data = [
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona",  "2.196222067", "41.38689041", "way/35816740", "7", "Q1425790"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", "2.218544006", "41.41080856", "way/206962976", "7", "Q5911239")]
        
        
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = kinds_bbox_struct
        
        expected_data = [
                ("W35816740" ,"skyscrapers,architecture,historic_architecture,accomodations,interesting_places,other_hotels,other_buildings_and_structures", "hotel arts barcelona",  "2.196222067", "41.38689041", "way/35816740", "7", "Q1425790", "7"),
                ("W206962976", "skyscrapers,architecture,accomodations,interesting_places,other_hotels", "barcelona princess", "2.218544006", "41.41080856", "way/206962976", "7", "Q5911239", "5")]
        
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = self.open_object.count_column_per_place(input_df, "kinds", "name")

        transformed_df.show()

        self.run_asserts(transformed_df, expected_df)

    def test_extra(self):
        
        input_schema = extra_fields_struct
        
        input_data = [("W35816740", "5", {"city": "Barcelona", "house": "Hotel Arts", "state": "CAT", "county": "BCN", "suburb": "la Vila Olímpica del Poblenou", "country": "España", "postcode": "8005", "pedestrian" : None, "country_code": "es", "house_number": "19-21", "city_district": "Ciutat Vella", "neighbourhood": None}, "https://www.booking.com/hotel/es/arts-barcelona.html", "https://commons.wikimedia.org/wiki/File:Hotel_Arts.jpg", "https://en.wikipedia.org/wiki/Hotel%20Arts"),
                      ("W206962976" , "4", {"city": "Barcelona", "house": "Hotel Barcelona Princess", "state": "CAT", "county": "BCN", "suburb": "el Besòs i el Maresme", "country": "España", "postcode": "8019", "pedestrian" : None, "country_code": "es", "house_number": "1", "city_district": "Sant Martí", "neighbourhood": None}, "https://www.booking.com/hotel/es/barcelonaprincess.html", "https://commons.wikimedia.org/wiki/File:Barcelona_-_Hotel_Barcelona_Princess_11.jpg", "https://en.wikipedia.org/wiki/Hotel%20Barcelona%20Princess"),
                ("W203140439", "4", {"city": "Barcelona", "house": "Meliá Barcelona Sky", "state": "CAT", "county": "BCN", "suburb": "Provençals del Poblenou", "country": "España", "postcode": "8005", "pedestrian" : None, "country_code": "es", "house_number": "272", "city_district": "Sant Martí", "neighbourhood": "Provençals del Poblenou"}, "https://www.booking.com/hotel/es/melia-barcelona-sky.html", "https://commons.wikimedia.org/wiki/File:PereIV-HabitatSky.jpg", "https://en.wikipedia.org/wiki/Hotel%20Melia%20Barcelona%20Sky"),
                ("N1272888981", "5", {"city": "l'Hospitalet", "house": "Hotel Hesperia Barcelona Tower", "state": "CAT", "county": "BCN", "suburb": "Bellvitge", "country": "España", "postcode": "8907", "pedestrian" : None, "country_code": "es", "house_number": "144", "city_district": "Districte VI", "neighbourhood": None}, "https://www.booking.com/hotel/es/hesperiatower.html", "https://commons.wikimedia.org/wiki/File:Spain.Catalonia.Hospitalet.Hotel.Hesperia.Plantilla.JPG", "https://en.wikipedia.org/wiki/Hyatt%20Regency%20Barcelona%20Tower"),
                ("W177405737", "4", {"city": "l'Hospitalet", "house": "Porta Fira", "state": "CAT", "county": "BCN", "suburb": "Granvia Sud", "country": "España", "postcode": "8908", "pedestrian" : None, "country_code": "es", "house_number": None, "city_district": "Districte III", "neighbourhood": None}, "https://www.booking.com/hotel/es/porta-fira.html", "https://commons.wikimedia.org/wiki/File:Barcelona_2010_August_005_Hotel.JPG", "https://en.wikipedia.org/wiki/Hotel%20Porta%20Fira")]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        expected_schema = flat_extra_field_struct
         
        expected_data = [("W35816740", "5", "Barcelona", "Hotel Arts", "CAT", "BCN", "la Vila Olímpica del Poblenou", "España", "8005", None, "es", "19-21", "Ciutat Vella", None, "https://www.booking.com/hotel/es/arts-barcelona.html", "https://commons.wikimedia.org/wiki/File:Hotel_Arts.jpg", "https://en.wikipedia.org/wiki/Hotel%20Arts"),
                ("W206962976" , "4", "Barcelona", "Hotel Barcelona Princess", "CAT", "BCN", "el Besòs i el Maresme", "España", "8019", None, "es", "1", "Sant Martí", None, "https://www.booking.com/hotel/es/barcelonaprincess.html", "https://commons.wikimedia.org/wiki/File:Barcelona_-_Hotel_Barcelona_Princess_11.jpg", "https://en.wikipedia.org/wiki/Hotel%20Barcelona%20Princess"),
                ("W203140439", "4", "Barcelona", "Meliá Barcelona Sky", "CAT", "BCN", "Provençals del Poblenou", "España", "8005", None, "es", "272", "Sant Martí", "Provençals del Poblenou", "https://www.booking.com/hotel/es/melia-barcelona-sky.html", "https://commons.wikimedia.org/wiki/File:PereIV-HabitatSky.jpg", "https://en.wikipedia.org/wiki/Hotel%20Melia%20Barcelona%20Sky"),
                ("N1272888981", "5", "l'Hospitalet", "Hotel Hesperia Barcelona Tower", "CAT", "BCN", "Bellvitge", "España", "8907", None, "es", "144", "Districte VI", None, "https://www.booking.com/hotel/es/hesperiatower.html", "https://commons.wikimedia.org/wiki/File:Spain.Catalonia.Hospitalet.Hotel.Hesperia.Plantilla.JPG", "https://en.wikipedia.org/wiki/Hyatt%20Regency%20Barcelona%20Tower"),
                ("W177405737", "4", "l'Hospitalet", "Porta Fira", "CAT", "BCN", "Granvia Sud", "España", "8908", None, "es", None, "Districte III", None, "https://www.booking.com/hotel/es/porta-fira.html", "https://commons.wikimedia.org/wiki/File:Barcelona_2010_August_005_Hotel.JPG", "https://en.wikipedia.org/wiki/Hotel%20Porta%20Fira")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = self.open_object.flat_extra_fields_df(input_df)

        self.run_asserts(transformed_df, expected_df)


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()