from pyspark.sql.types import StringType, StructField, MapType, StructType, IntegerType

"""
Schema Definitions.
"""

bbox_struct = StructType([
            StructField('xid', StringType()),
            StructField('kinds', StringType()),
            StructField('name', StringType()),
            StructField('point', MapType(
                StringType(), StringType(), False
            )),
            StructField('osm', StringType()),
            StructField('rate', StringType()),
            StructField('wikidata', StringType())
        ])

flat_bbox_struct = StructType([
        StructField('xid', StringType()),
        StructField('kinds', StringType()),
        StructField('name', StringType()),
        StructField('lon', StringType()),
        StructField('lat', StringType()),
        StructField('osm', StringType()),
        StructField('rate', StringType()),
        StructField('wikidata', StringType())
        ])

kinds_bbox_struct = StructType([
        StructField('xid', StringType()),
        StructField('kinds', StringType()),
        StructField('name', StringType()),
        StructField('lon', StringType()),
        StructField('lat', StringType()),
        StructField('osm', StringType()),
        StructField('rate', StringType()),
        StructField('wikidata', StringType()),
        StructField('kinds_amount', IntegerType(), False)
        ])

extra_fields_struct = StructType([
        StructField('xid', StringType()),
        StructField('stars', StringType()),
        StructField('address', MapType(
            StringType(), StringType(), True
        )),
        StructField('url', StringType()),
        StructField('image', StringType()),
        StructField('wikipedia', StringType())
    ])

flat_extra_field_struct = StructType([
        StructField('xid', StringType()),
        StructField('stars', StringType()),
        StructField('city', StringType()),
        StructField('house', StringType()),
        StructField('state', StringType()),
        StructField('county', StringType()),
        StructField('suburb', StringType()),
        StructField('country', StringType()),
        StructField('postcode', StringType()),
        StructField('pedestrian', StringType()),
        StructField('country_code', StringType()),
        StructField('house_number', StringType()),
        StructField('city_district', StringType()),
        StructField('neighbourhood', StringType()),
        StructField('url', StringType()),
        StructField('image', StringType()),
        StructField('wikipedia', StringType())
        ])

final_struct = StructType([
        StructField('xid', StringType()),
        StructField('kinds', StringType()),
        StructField('name', StringType()),
        StructField('lon', StringType()),
        StructField('lat', StringType()),
        StructField('osm', StringType()),
        StructField('rate', StringType()),
        StructField('wikidata', StringType()),
        StructField('kinds_amount', IntegerType(), False),
        StructField('stars', StringType()),
        StructField('city', StringType()),
        StructField('house', StringType()),
        StructField('state', StringType()),
        StructField('county', StringType()),
        StructField('suburb', StringType()),
        StructField('country', StringType()),
        StructField('postcode', StringType()),
        StructField('pedestrian', StringType()),
        StructField('country_code', StringType()),
        StructField('house_number', StringType()),
        StructField('city_district', StringType()),
        StructField('neighbourhood', StringType()),
        StructField('url', StringType()),
        StructField('image', StringType()),
        StructField('wikipedia', StringType())
        ])