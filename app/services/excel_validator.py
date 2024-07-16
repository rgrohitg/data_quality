import pandas as pd
from pyspark.sql import SparkSession

from app.core.spark_session import SparkSessionFactory
from app.models.models import ValidationResult
from app.services.file_validator import FileValidator
from app.utils.validation_utils import load_specification, execute_validation

class ExcelValidator(FileValidator):

    def validate(self, data_file_path: str, spec_file_path: str) -> ValidationResult:
        spec = load_specification(spec_file_path)
        spark = SparkSessionFactory.get_spark_session()
        return execute_validation(spark, spec, spec_file_path, data_file_path, self.read_excel_with_schema)

    def read_excel_with_schema(self, spark: SparkSession, data_file_path: str, sheet_name: str):
        pdf = pd.read_excel(data_file_path, sheet_name=sheet_name)
        return spark.createDataFrame(pdf)
