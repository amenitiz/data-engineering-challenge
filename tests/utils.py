import json
import logging
import unittest
from pathlib import Path
from typing import Dict

from pyspark.sql.dataframe import  DataFrame

from src.open_trip_map import new_spark_session


class PySparkTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)
        cls.spark = new_spark_session('test_pipeline')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()


def test_data(df1: DataFrame, df2: DataFrame) -> bool:
    """
    Verifies if the data en df1 is equal to the data in df2
    """

    data1 = df1.collect()
    data2 = df2.collect()

    return set(data1) == set(data2)


def test_schema(df1: DataFrame, df2: DataFrame) -> bool:
    """
    Verifies if df1 and df2 has the same field names and data types
    """

    field_list = lambda fields: (fields.name, fields.dataType)

    df1_fields = [*map(field_list, df1.schema.fields)]
    df2_fields = [*map(field_list, df2.schema.fields)]

    result = set([field[:-1] for field in df1_fields]) == set([field[:-1] for field in df2_fields])

    return result


def parse_json_file(file_path: str) -> Dict[str, str]:

    if not Path(file_path).exists():
        raise FileNotFoundError(f'Cannot find json file in {file_path}')

    with open(file_path, mode='r', encoding='utf-8') as f:
        json_contents = json.load(f)

    return json_contents
