# app/core/aws_client.py
import boto3
from app.core.config import settings

class AWSClient:
    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region_name
        )
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region_name
        )

    def get_item_from_dynamodb(self, table_name, key):
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key=key)
        return response.get('Item')

    def get_file_from_s3(self, bucket_name, key):
        response = self.s3.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')

aws_client = AWSClient()
