import json
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DoubleType


def load_specification(spec_file_path: str) -> list:
    with open(spec_file_path, 'r') as spec_file:
        return json.load(spec_file)


def map_json_type_to_spark_type(column: dict) -> type:
    """
    Maps a JSON type to its corresponding PySpark SQL type.

    Parameters:
    column (dict): A dictionary representing a column with a 'source_col_type' key.

    Returns:
    type: The corresponding PySpark SQL type for the given JSON type.

    Raises:
    ValueError: If the given JSON type is not supported.
    """
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
    """
    Creates a PySpark SQL schema from a given specification.

    The function extracts column details from the specification, maps the JSON types to their corresponding PySpark SQL types,
    and constructs a StructType object with the extracted fields.

    Parameters:
    spec (List): A list representing the specification. The specification should contain details about the columns,
                 including their names and types.

    Returns:
    StructType: A PySpark SQL StructType object representing the schema.

    Raises:
    IndexError: If the specification does not contain the expected structure.
    """
    columns = spec[0]["details"]["sheets"][0]["columns"]
    fields = [StructField(col["source_col_name"], map_json_type_to_spark_type(col), False) for col in columns]
    return StructType(fields)


def read_csv_with_schema(spark: SparkSession, data_file_path: str, schema: StructType) -> DataFrame:
    return spark.read.csv(data_file_path, sep=",", header=True, mode="PERMISSIVE",
                                         columnNameOfCorruptRecord="_corrupt_record")
