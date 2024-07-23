import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

from app.core.spark_session import SparkSessionFactory
from app.models.models import ValidationResult
from app.models.requests import S3FileRequest
from app.services.file_validator import FileValidator
from app.utils.validation_utils import DataValidator
from app.logger_config import logger

class CSVValidator(FileValidator):
    def validate(self, request: S3FileRequest) -> dict:
        logger.info(f"Starting CSV validation for data file {request.s3TempKey} with specification {request.specKey}")
        validator = DataValidator(request.specKey, request.s3TempKey)
        return validator.execute_validation(self.read_csv_with_schema)

    def read_csv_with_schema(self, spark: SparkSession, data_file_path: str, sheet_name: str):
        df = spark.read.csv(data_file_path, sep=",", header=True, inferSchema=True)

        # Add an index column starting from 1
        df_with_index = df.withColumn("ROW", monotonically_increasing_id() + 1)
        df_with_index.show()
        return df_with_index

