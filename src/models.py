from pyspark.sql.types import DoubleType, StringType, StructField, StructType, MapType

Place = StructType([
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


PlaceDetails = StructType([
    StructField('xid', StringType()),
    StructField('stars', StringType()),
    StructField('address', MapType(
        StringType(), StringType(), False
    )),
    StructField('url', StringType()),
    StructField('image', StringType()),
    StructField('wikipedia', StringType())
])
