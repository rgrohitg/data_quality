import pandas as pd
from pyspark.sql import SparkSession

from app.core.spark_session import SparkSessionFactory
from app.models.models import ValidationResult
from app.services.file_validator import FileValidator
from app.utils.validation_utils import DataValidator

class ExcelValidator(FileValidator):

    def validate(self, data_file_path: str, spec_file_path: str) -> ValidationResult:
        validator = DataValidator(spec_file_path, data_file_path)

        return validator.execute_validation(self.read_excel_with_schema)

    def read_excel_with_schema(self, spark: SparkSession, data_file_path: str, sheet_name: str):
        pdf = pd.read_excel(data_file_path, sheet_name=sheet_name)
        return spark.createDataFrame(pdf)
