import json
import logging
from pyspark.sql import SparkSession, DataFrame

from app.core.spark_session import SparkSessionFactory
from app.services.converter import convert_spec_to_soda_cl
from app.services.soda_scan import configure_and_execute_scan, CustomSampler
from app.models.models import ValidationResult
from app.logger_config import logger


def load_specification(spec_file_path: str) -> dict:
    logger.info(f"Loading specification from {spec_file_path}")
    with open(spec_file_path, 'r') as spec_file:
        spec = json.load(spec_file)
        logger.debug(f"Specification loaded: {spec}")
        return spec


def initialize_spark_session() -> SparkSession:
    return SparkSessionFactory.get_spark_session()


def execute_validation(spark: SparkSession, spec: dict, spec_file_path: str, data_file_path: str,
                       read_func) -> ValidationResult:
    validation_results = {}
    for sheet in spec[0]["details"]["sheets"]:
        logger.info(f"Processing sheet: {sheet['sheet_name']}")
        data_file = read_func(spark, data_file_path, sheet['sheet_name'])
        data_file.createOrReplaceTempView(sheet["table_name"])

        soda_check_path = convert_spec_to_soda_cl(sheet, "./data/soda_conversion_template.yml")
        custom_sampler = CustomSampler()
        logger.debug(f"soda_check_path: {soda_check_path}")
        validation_results[sheet['sheet_name']] = configure_and_execute_scan(spark, soda_check_path, custom_sampler,
                                                                             spec_file_path, data_file_path)

    SparkSessionFactory.stop_spark_session()  # Ensure the Spark session is stopped after use
    is_valid_file = all(res.is_valid_file for res in validation_results.values())
    validation_result = ValidationResult(
        is_valid_file=is_valid_file,
        file_details={
            "spec_key": spec_file_path,
            "data_file": data_file_path,
        },
        errors={sheet: res.errors for sheet, res in validation_results.items()},
        success={sheet: res.success for sheet, res in validation_results.items()}
    )
    logger.info(f"Validation result: {validation_result}")
    return validation_result
