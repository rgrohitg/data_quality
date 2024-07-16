# app/models/dynamodb_models.py

from pydantic import BaseModel, Field
from typing import Dict, List
from datetime import datetime

class ValidationRun(BaseModel):
    run_id: str
    tenant_key: str
    dynamodb_key: str
    s3_temp_key: str
    file_name: str
    file_id: str
    status: str
    created_at: datetime
    updated_at: datetime

class ValidationResult(BaseModel):
    result_id: str
    run_id: str
    validator_type: str
    validation_details: str
    errors: Dict
    success: Dict
    created_at: datetime
    updated_at: datetime
