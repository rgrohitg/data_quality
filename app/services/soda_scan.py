from typing import Dict

from pyspark.sql import SparkSession
from soda.scan import Scan
from soda.common.logs import configure_logging
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext

from app.models.models import ValidationResult


class CustomSampler(Sampler):
    """
    CustomSampler is a class that implements the Sampler interface to store samples of data during a Soda scan.

    Attributes:
    - errors: A dictionary to store failed rows for each check.
    - success: A dictionary to store successful queries.

    Methods:
    - store_sample: Stores a sample of data based on the given SampleContext.
    """

    def __init__(self):
        """
        Initializes the CustomSampler with empty errors and success dictionaries.
        """
        self.errors: dict = {}
        self.success: dict = {}

    def store_sample(self, sample_context: SampleContext):
        """
        Stores a sample of data based on the given SampleContext.

        Parameters:
        - sample_context (SampleContext): The context of the sample, including the check name, sample data, and query.

        Returns:
        - None
        """
        rows = sample_context.sample.get_rows()
        print("FAILED ROWS:{rows}", rows)
        if rows:
            self.errors[sample_context.check_name] = {
                "Failed Rows": rows
            }
        else:
            self.success[sample_context.query] = "Passed"

    def get_validation_result(self, spec_file_path: str, data_file_path: str) -> ValidationResult:
        return ValidationResult(
            is_valid_file=not bool(self.errors),  # If there are errors, the file is not valid
            file_details={
                "spec_key": spec_file_path,
                "data_file": data_file_path,
            },
            errors=self.errors,
            success=self.success
        )


def configure_and_execute_scan(spark: SparkSession, soda_check_path: str, custom_sampler: CustomSampler, spec_file_path: str, data_file_path: str) -> ValidationResult:
    """
    Configures and executes a Soda scan using the provided Spark session, Soda check path, and custom sampler.

    Parameters:
    - spark (SparkSession): The Spark session to be used for the scan.
    - soda_check_path (str): The path to the Soda check YAML file.
    - custom_sampler (CustomSampler): An instance of the custom sampler class to store samples of data during the scan.

    Returns:
    - dict: A dictionary containing the scan results.
    """
    configure_logging()
    scan = Scan()
    scan.sampler = custom_sampler
    scan.set_scan_definition_name("CSV_FILE_TEST")
    scan.set_data_source_name(data_source_name="spark_df")
    scan.add_spark_session(spark_session=spark)
    scan.add_sodacl_yaml_file(file_path=soda_check_path)

    scan.set_verbose(True)
    scan.execute()

    return custom_sampler.get_validation_result(spec_file_path, data_file_path)
