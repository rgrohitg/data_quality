# validator.py
from app.services.file_handler import load_specification, create_schema_from_spec, read_csv_with_schema
from app.services.soda_scan import configure_and_execute_scan, CustomSampler
from app.services.shutdown import get_spark_session
from app.models.models import ValidationResult


def validate_data(data_file_path: str, soda_check_path: str, spec_file_path: str) -> ValidationResult:
    """
    Validates a CSV file against a Soda check using a given specification.

    Parameters:
    data_file_path (str): The path to the CSV file to be validated.
    soda_check_path (str): The path to the Soda check configuration file.
    spec_file_path (str): The path to the specification JSON file.

    Returns:
    ValidationResult: An instance containing the validation results.
    """
    print("SODA_CHECK_PATH:", soda_check_path)

    # Load specification JSON
    spec = load_specification(spec_file_path)

    # Initialize Spark session
    spark = get_spark_session()

    # Create schema from specification
    schema = create_schema_from_spec(spec)

    # Read CSV file with schema
    csv_file = read_csv_with_schema(spark, data_file_path, schema)
    csv_file.printSchema()
    csv_file.createOrReplaceTempView(spec[0]["details"]["sheets"][0]["table_name"])
    csv_file.show()

    # Configure and execute Soda scan with custom sampler
    custom_sampler = CustomSampler()
    validation_results = configure_and_execute_scan(spark, soda_check_path, custom_sampler)

    # Build ValidationResult instance
    result = ValidationResult(
        is_valid_file=not bool(custom_sampler.errors),
        file_details={
            "spec_key": spec_file_path,
            "data_file": data_file_path,
            "soda_check": soda_check_path
        },
        errors=custom_sampler.errors,
        success=custom_sampler.success,
        soda_scan_results=validation_results
    )

    return result
