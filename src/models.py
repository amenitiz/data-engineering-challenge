from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, MapType

# Schema returned by opentripapi bbox rest endpoint
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

# Schema after flatten point field on PlaceRaw
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

# Schema returned by xid endpoint filtering the fields needed for the pipeline
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

# Resulting schema after flattening PlaceDetailsRaw address field
PlaceDetails = StructType([
    StructField('xid', StringType()),
    StructField('stars', IntegerType()),
    StructField('url', StringType()),
    StructField('image', StringType()),
    StructField('wikipedia', StringType()),
    StructField('country_code', StringType()),
    StructField('country', StringType()),
    StructField('city', StringType()),
    StructField('postcode', StringType()),
    StructField('county', StringType()),
    StructField('suburb', StringType()),
    StructField('house_number', IntegerType()),
    StructField('pedestrian', StringType()),
    StructField('state', StringType()),
    StructField('city_district', StringType())
])
