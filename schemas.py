from pyspark.sql.types import StringType, StructField, MapType, StructType 

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

extra_fields_struct = StructType([
        StructField('xid', StringType()),
        StructField('stars', StringType()),
        StructField('address', MapType(
            StringType(), StringType(), False
        )),
        StructField('url', StringType()),
        StructField('image', StringType()),
        StructField('wikipedia', StringType())
    ])