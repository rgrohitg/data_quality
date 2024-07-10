# models.py
from pydantic import BaseModel
from typing import Dict


class DataValidationRequest(BaseModel):
    spec_key: str
    data_file: str


class ValidationResult(BaseModel):
    is_valid_file: bool
    file_details: Dict[str, str]
    errors: Dict
    success: Dict
    soda_scan_results: Dict
