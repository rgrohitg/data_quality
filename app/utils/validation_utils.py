import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import DataFrame
from app.core.spark_session import SparkSessionFactory
from app.services.converter import convert_spec_to_soda_cl
from app.services.soda_scan import configure_and_execute_scan, CustomSampler
from app.models.models import ValidationResult
from app.logger_config import logger

class DataValidator:
    def __init__(self, spec_file_path: str, data_file_path: str):
        self.spec_file_path = spec_file_path
        self.data_file_path = data_file_path
        self.spark = self.initialize_spark_session()

    def initialize_spark_session(self) -> SparkSession:
        return SparkSessionFactory.get_spark_session()

    def load_specification(self) -> dict:
        logger.info(f"Loading specification from {self.spec_file_path}")
        with open(self.spec_file_path, 'r') as spec_file:
            spec = json.load(spec_file)
            logger.debug(f"Specification loaded: {spec}")
            return spec

    def execute_validation(self, read_func) -> ValidationResult:
        spec = self.load_specification()
        validation_results = {}
        for sheet in spec[0]["details"]["sheets"]:
            logger.info(f"Processing sheet: {sheet['sheet_name']}")
            data_file = read_func(self.spark, self.data_file_path, sheet['sheet_name'])
            data_file.createOrReplaceTempView(sheet["table_name"])

            soda_check_path = convert_spec_to_soda_cl(sheet, "./data/soda_conversion_template.yml")
            custom_sampler = CustomSampler(self.spark)
            logger.debug(f"soda_check_path: {soda_check_path}")
            validation_results[sheet['sheet_name']] = configure_and_execute_scan(
                self.spark, soda_check_path, custom_sampler, self.spec_file_path, self.data_file_path
            )

        SparkSessionFactory.stop_spark_session()  # Ensure the Spark session is stopped after use
        is_valid_file = all(res.is_valid_file for res in validation_results.values())
        validation_result = ValidationResult(
            is_valid_file=is_valid_file,
            file_details={
                "spec_key": self.spec_file_path,
                "data_file": self.data_file_path,
            },
            errors={sheet: res.errors for sheet, res in validation_results.items()},
            success={sheet: res.success for sheet, res in validation_results.items()}
        )
        logger.info(f"Validation result: {validation_result}")
        return validation_result
