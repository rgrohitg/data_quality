from app.services.csv_validator import CSVValidator
from app.services.excel_validator import ExcelValidator


class FileValidatorFactory:

    @staticmethod
    def get_validator(file_type: str):
        if file_type == 'csv':
            return CSVValidator()
        elif file_type == 'excel':
            return ExcelValidator()
        else:
            raise ValueError("Unsupported file type")
