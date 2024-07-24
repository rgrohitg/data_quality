from fastapi import APIRouter, HTTPException, Depends
from requests import Session

# from app.data.dependencies import get_db
from app.models.requests import S3FileRequest
# from app.services.file_processor import file_processor
from app.services.file_validator_factory import FileValidatorFactory
from app.models.models import ValidationResult, DataValidationRequest

router = APIRouter()


@router.post("/validate", response_model=dict)
def validate_data(request: S3FileRequest) -> dict:
    try:
        # FETCHING SPECIFICATION FILE FROM DYNAMO DB  BASED ON specKey
        # db.get_table_with_filters("tablenmae",{},{})

        # Checking s3TempKey file splitting based on . and taking file extension to file_type
        # TODO need to read the s3 file based on s3TempKey where should i do it?
        validator = FileValidatorFactory.get_validator("csv")
        validation_result = validator.validate(request)
        print("-------------------%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%-------------------")
        print(validation_result)
        return validation_result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
