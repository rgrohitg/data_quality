# app/models/requests.py
from pydantic import BaseModel

class S3FileRequest(BaseModel):
    tenantKey: str
    dynamoDbKey: str
    s3TempKey: str
    specKey: str
    fileName: str
    fileId: str
    message: str
    user: str
