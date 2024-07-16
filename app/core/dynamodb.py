# app/config/dynamodb.py

import boto3
from pydantic import BaseSettings

class DynamoDBSettings(BaseSettings):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str

    class Config:
        env_file = ".env"

settings = DynamoDBSettings()

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
    region_name=settings.region_name
)

validation_runs_table = dynamodb.Table('ValidationRuns')
validation_results_table = dynamodb.Table('ValidationResults')
