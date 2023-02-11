from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, MapType

PlaceRaw = StructType([
    StructField('xid', StringType()),
    StructField('name', StringType()),
    StructField('highlighted_name', StringType()),
    StructField('kinds', StringType()),
    StructField('osm', StringType()),
    StructField('wikidata', StringType()),
    StructField('dist', DoubleType()),
    StructField('point', MapType(
        StringType(), DoubleType(), False
    ))
])

PlaceFlattened = StructType([
    StructField('xid', StringType()),
    StructField('name', StringType()),
    StructField('highlighted_name', StringType()),
    StructField('kinds', StringType()),
    StructField('osm', StringType()),
    StructField('wikidata', StringType()),
    StructField('dist', DoubleType()),
    StructField('lon', DoubleType()),
    StructField('lat', DoubleType())
])


PlaceDetailsRaw = StructType([
    StructField('xid', StringType()),
    StructField('stars', IntegerType()),
    StructField('address', MapType(
        StringType(), StringType(), False
    )),
    StructField('url', StringType()),
    StructField('image', StringType()),
    StructField('wikipedia', StringType())
])

PlaceDetails = StructType([
    StructField('xid', StringType()),
    StructField('stars', IntegerType()),
    StructField('url', StringType()),
    StructField('image', StringType()),
    StructField('wikipedia', StringType()),
    StructField('country_code', StringType()),
    StructField('country', StringType()),
    StructField('city', StringType()),
    StructField('postcode', IntegerType()),
    StructField('county', StringType()),
    StructField('suburb', StringType()),
    StructField('house_number', IntegerType()),
    StructField('pedestrian', StringType()),
    StructField('state', StringType()),
    StructField('city_district', StringType())
])
