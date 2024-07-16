from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from app.models.models import ValidationResult

class FileValidator(ABC):

    @abstractmethod
    def validate(self, data_file_path: str, spec_file_path: str) -> ValidationResult:
        pass
