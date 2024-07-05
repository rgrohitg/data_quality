from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.services.converter import convert_spec_to_soda_cl
from app.services.validator import validate_data

import os


class DataValidationRequest(BaseModel):
    spec_key: str
    data_file: str


router = APIRouter()


@router.post("/validate")
async def validate(request: DataValidationRequest):
    """
    Validates data against a given specification using SodaCL.

    Parameters:
    request (DataValidationRequest): The request object containing the specification key and data file path.

    Returns:
    dict: A dictionary containing the status and validation results.

    Raises:
    HTTPException: If an error occurs during the validation process.
    """
    try:
        spec_path = request.spec_key  # For phase 1, assume this is a local path
        data_file_path = request.data_file  # For phase 1, assume this is a local path

        # Convert specification to SodaCL check YAML
        soda_check_path = convert_spec_to_soda_cl(spec_path)
        print(soda_check_path)

        # Validate the data file against the SodaCL checks
        validation_results = validate_data(data_file_path, soda_check_path)

        return {"status": "success", "results": validation_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
