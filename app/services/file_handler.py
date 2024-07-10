import json
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DoubleType


def load_specification(spec_file_path: str) -> list:
    with open(spec_file_path, 'r') as spec_file:
        return json.load(spec_file)


def map_json_type_to_spark_type(column: dict):
    json_type = column["source_col_type"].lower()
    if json_type == "text":
        return StringType()
    elif json_type == "decimal":
        return DecimalType()
    elif json_type == "integer":
        return IntegerType()
    elif json_type == "double":
        return DoubleType()
    else:
        raise ValueError(f"Unsupported type: {json_type}")


def create_schema_from_spec(spec: List) -> StructType:
    columns = spec[0]["details"]["sheets"][0]["columns"]
    fields = [StructField(col["source_col_name"], map_json_type_to_spark_type(col), False) for col in columns]
    return StructType(fields)


def read_csv_with_schema(spark: SparkSession, data_file_path: str, schema: StructType) -> DataFrame:
    return spark.read.csv(data_file_path, sep=",", header=True, mode="PERMISSIVE",
                                         columnNameOfCorruptRecord="_corrupt_record")
