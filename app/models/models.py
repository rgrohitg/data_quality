# models.py
from pydantic import BaseModel
from typing import Dict


class DataValidationRequest(BaseModel):
    spec_file_path: str
    data_file_path: str
    file_type: str


class ValidationResult(BaseModel):
    is_valid_file: bool
    file_details: Dict[str, str]
    errors: Dict
    success: Dict
