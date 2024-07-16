# app/services/validation_service.py
from app.services.dynamodb_client import DynamoDBClient
from app.models.dynamodb_models import ValidationRun, ValidationResult
from app.core.spark_session import SparkSessionFactory
from app.services.file_validator_factory import FileValidatorFactory
from app.models.models import ValidationResult as ValidationResultModel
from app.soda_scan import configure_and_execute_scan
from datetime import datetime
import uuid

class ValidationService:
    def __init__(self):
        self.dynamodb_client = DynamoDBClient()

    def start_validation(self, request_data):
        run_id = str(uuid.uuid4())
        job_id = request_data['fileId']
        run = ValidationRun(
            run_id=run_id,
            job_id=job_id,
            tenant_key=request_data['tenantKey'],
            status="PENDING",
            start_time=datetime.utcnow().isoformat(),
            created_by=request_data['user']
        )
        self.dynamodb_client.create_validation_run(run)

        # Fetch specification and file paths (mocked for this example)
        spec_file_path = self.fetch_spec_file_from_dynamodb(request_data['dynamoDbKey'])
        data_file_path = self.fetch_file_from_s3(request_data['s3TempKey'])

        # Update run status to IN_PROGRESS
        self.dynamodb_client.update_validation_run(run_id, {'status': 'IN_PROGRESS'})

        # Perform validation
        validator = FileValidatorFactory.get_validator(request_data['fileName'].split('.')[-1])
        validation_result: ValidationResultModel = validator.validate(data_file_path, spec_file_path)

        # Update validation run with results
        result = ValidationResult(
            result_id=str(uuid.uuid4()),
            run_id=run_id,
            errors=validation_result.errors,
            success=validation_result.success
        )
        self.dynamodb_client.create_validation_result(result)

        # Finalize the validation run
        updates = {
            'status': 'COMPLETED' if validation_result.is_valid_file else 'FAILED',
            'end_time': datetime.utcnow().isoformat(),
            'validation_result': 'Pass' if validation_result.is_valid_file else 'Fail',
            'last_processed_step': 'ValidationCompleted'
        }
        self.dynamodb_client.update_validation_run(run_id, updates)

    def fetch_spec_file_from_dynamodb(self, dynamoDbKey: str) -> str:
        # Mocked response
        return './data/specification_csv.json'

    def fetch_file_from_s3(self, s3TempKey: str) -> str:
        # Mocked response
        return './data/data.csv'
