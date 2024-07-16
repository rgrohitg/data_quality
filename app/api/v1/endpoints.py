from fastapi import APIRouter, HTTPException
from app.services.file_validator_factory import FileValidatorFactory
from app.models.models import ValidationResult, DataValidationRequest

router = APIRouter()


@router.post("/validate", response_model=ValidationResult)
def validate_data(request: DataValidationRequest) -> ValidationResult:
    try:
        validator = FileValidatorFactory.get_validator(request.file_type)
        validation_result = validator.validate(request.data_file_path, request.spec_file_path)
        return validation_result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/process-file", response_model=ValidationResult)
def process_file(request: S3FileRequest) -> ValidationResult:
    try:
        validation_result = file_processor.process_file(request.dynamoDbKey, request.s3TempKey)
        return validation_result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))