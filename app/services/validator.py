from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from soda.scan import Scan
from soda.common.logs import configure_logging


def validate_data(data_file_path: str, soda_check_path: str) -> dict:
    """
    Validates the data in a CSV file using Soda library.

    Parameters:
    data_file_path (str): The path to the CSV file to be validated.
    soda_check_path (str): The path to the Soda check YAML file.

    Returns:
    dict: A dictionary containing the validation results.

    """
    print("SODA_CHECK_PATH:", soda_check_path)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SodaQualityChecks") \
        .getOrCreate()

    # Read the CSV data into a Spark DataFrame
    csv_file: DataFrame = spark.read.csv(data_file_path, sep=",", header=True, inferSchema=True)
    csv_file.printSchema()
    csv_file.createOrReplaceTempView("my_df")

    # Create Soda scan
    configure_logging()
    scan = Scan()
    scan.set_scan_definition_name("CSV_FILE_TEST")
    scan.add_sodacl_yaml_file(file_path="./data/soda_check.yml")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark)
    scan.set_verbose(True)

    # Run the scan
    scan_results = scan.execute()
    print(scan.get_scan_results())

    # Convert scan results to dictionary for returning
    validation_results = parse_scan_results(scan)

    return validation_results


def parse_scan_results(scan):
    validation_results = {
        "summary": "Validation completed successfully.",
        "checks": []
    }

    # Example of extracting details from scan results
    for check in scan.get_checks():
        check_result = {
            "name": check.check_name,
            "status": check.outcome,
            "failed_rows": check.failed_rows_sample if hasattr(check, 'failed_rows_sample') else []
        }
        validation_results["checks"].append(check_result)

    return validation_results
