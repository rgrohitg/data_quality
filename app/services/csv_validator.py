import logging
from pyspark.sql import SparkSession

from app.core.spark_session import SparkSessionFactory
from app.models.models import ValidationResult
from app.services.file_validator import FileValidator
from app.utils.validation_utils import load_specification, execute_validation
from app.logger_config import logger

class CSVValidator(FileValidator):

    def validate(self, data_file_path: str, spec_file_path: str) -> ValidationResult:
        logger.info(f"Starting CSV validation for data file {data_file_path} with specification {spec_file_path}")
        spec = load_specification(spec_file_path)
        spark = SparkSessionFactory.get_spark_session()
        return execute_validation(spark, spec, spec_file_path, data_file_path, self.read_csv_with_schema)

    def read_csv_with_schema(self, spark: SparkSession, data_file_path: str, sheet_name: str):
        logger.info(f"Reading CSV file from {data_file_path}")
        df = spark.read.csv(data_file_path, sep=",", header=True, inferSchema=True)
        logger.debug(f"CSV file schema: {df.schema}")
        return df
