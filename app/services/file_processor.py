# app/services/file_processor.py
from app.core.aws_client import aws_client
from app.services.file_validator_factory import FileValidatorFactory
from app.models.models import ValidationResult
import json

class FileProcessor:
    @staticmethod
    def process_file(dynamo_db_key: str, s3_temp_key: str) -> ValidationResult:
        # Mock the DynamoDB response for now
        spec_file = json.loads(aws_client.get_item_from_dynamodb(settings.dynamodb_table_name, {'spec_key': dynamo_db_key}))

        # Read the file from S3
        data_file = aws_client.get_file_from_s3(settings.s3_bucket_name, s3_temp_key)

        # Determine file type
        file_type = 'csv' if data_file.endswith('.csv') else 'excel'

        # Get the appropriate validator
        validator = FileValidatorFactory.get_validator(file_type)

        # Perform validation
        validation_result = validator.validate(data_file_path=s3_temp_key, spec_file_path=dynamo_db_key)

        return validation_result

file_processor = FileProcessor()
