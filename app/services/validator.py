import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DoubleType
from soda.scan import Scan
from soda.common.logs import configure_logging


def load_specification(spec_file_path: str) -> list:
    with open(spec_file_path, 'r') as spec_file:
        return json.load(spec_file)


def map_json_type_to_spark_type(column: dict):
    json_type = column["source_col_type"].lower()
    if json_type == "text":
        return StringType()
    elif json_type == "decimal":
        # precision = column.get("precision", 10)
        # scale = column.get("scale", 2)
        # return DecimalType(precision, scale)
        return DecimalType()
    elif json_type == "integer":
        return IntegerType()
    elif json_type == "double":
        return DoubleType()
    else:
        raise ValueError(f"Unsupported type: {json_type}")


def create_schema_from_spec(spec: dict) -> StructType:
    columns = spec[0]["details"]["sheets"][0]["columns"]
    fields = [StructField(col["source_col_name"], map_json_type_to_spark_type(col), False) for col in columns]
    return StructType(fields)


def read_csv_with_schema(spark: SparkSession, data_file_path: str, schema: StructType) -> DataFrame:
    return spark.read.csv(data_file_path, sep=",", header=True, mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")


def configure_and_execute_scan(spark: SparkSession, soda_check_path: str) -> dict:
    configure_logging()
    scan = Scan()
    scan.set_scan_definition_name("CSV_FILE_TEST")
    scan.set_data_source_name(data_source_name="spark_df")
    scan.add_spark_session(spark_session=spark)
    scan.add_sodacl_yaml_file(file_path=soda_check_path)
    scan.set_verbose(True)
    scan.execute()
    return scan.get_scan_results()


def validate_data(data_file_path: str, soda_check_path: str, spec_file_path: str) -> dict:
    print("SODA_CHECK_PATH:", soda_check_path)

    # Load specification JSON
    spec = load_specification(spec_file_path)

    # Initialize Spark session
    spark = (SparkSession.builder.appName("SodaQualityChecks")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.memory", "2g")
             .config("spark.local.dir", "./data")
             .getOrCreate())

    # Create schema from specification
    schema = create_schema_from_spec(spec)

    # Read CSV file with schema
    csv_file = read_csv_with_schema(spark, data_file_path, schema)
    csv_file.printSchema()
    csv_file.createOrReplaceTempView(spec[0]["details"]["sheets"][0]["table_name"])
    csv_file.show()
    # Configure and execute Soda scan
    validation_results = configure_and_execute_scan(spark, soda_check_path)

    return validation_results



# # Example usage
# data_file_path = "./data/data.csv"
# soda_check_path = "./data/soda_check.yml"
# spec_file_path = "./data/specification.json"
# validation_results = validate_data(data_file_path, soda_check_path, spec_file_path)
# print(validation_results)
